import argparse             
import json                
import os                   
import socket               
import struct                
import threading            
import time                 
from pathlib import Path    

from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout

UDP_PORT = 9999
DOWNLOAD_DIR = Path("downloads")

peers = {}         # tên -> {name, ip, port, status, last_seen}
peer_sockets = {}  # tên -> socket chat đang mở
histories = {}     # tên -> danh sách tin nhắn
unread = {}        # tên -> số tin chưa đọc
current = {"name": None}
me = {"name": "", "port": 0, "status": "online"}
lock = threading.RLock()


def log(message):
    print(f"\n{message}", flush=True)

def send_json(sock, obj):
    """Gui object JSON theo dinh dang [4 byte do dai][payload]."""
    # Chuyen object Python thanh chuoi JSON roi ma hoa thanh UTF-8.
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    # Gui header truoc de ben nhan biet chinh xac kich thuoc payload.
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_exact(sock, n):
    #Doc dung n byte vi TCP co the tra ve it byte hon yeu cau.
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Kết nối đã đóng")
        buf += chunk
    return buf


def recv_json(sock):
    """Doc header, doc payload va giai ma payload thanh dictionary."""
    size = struct.unpack("!I", recv_exact(sock, 4))[0]
    if size > 64 * 1024:
        raise ValueError("Gói tin quá lớn")
    obj = json.loads(recv_exact(sock, size))
    if not isinstance(obj, dict):
        raise ValueError("Gói tin không hợp lệ")
    return obj

def udp_broadcaster():
    """Dinh ky broadcast thong tin cua minh de cac may khac tim thay."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    while True:
        msg = f"HELLO|{me['name']}|{me['port']}|{me['status']}".encode()
        for addr in ("255.255.255.255", "127.255.255.255", "192.168.1.255", "10.144.160.255"):
            try:
                sock.sendto(msg, (addr, UDP_PORT))
            except OSError as e:
                log(f"[UDP] Không gửi được tới {addr}:{UDP_PORT}: {e}")
        time.sleep(3)


def udp_listener():
    """Lang nghe broadcast, cap nhat danh sach peers va loai peer offline."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", UDP_PORT))
    sock.settimeout(1)
    while True:
        try:
            # Nhan toi da 1024 byte tu mot goi UDP.
            data, addr = sock.recvfrom(1024)
            tag, name, port, status = data.decode().split("|")
            if tag == "HELLO" and name != me["name"]:
                # Khoa khi cap nhat dict dung chung voi cac thread khac.
                with lock:
                    is_new = name not in peers
                    peers[name] = {"name": name,
                                   "ip": addr[0],
                                   "port": int(port),
                                   "status": status,
                                   "last_seen": time.time()}
                if is_new:
                    log(f"[*] {name} đã online")
        except (socket.timeout, ValueError):
            pass
        with lock:
            for name in [n for n, p in peers.items() if time.time() - p["last_seen"] > 10]:
                del peers[name]
                log(f"[*] {name} đã offline")


def receive_file(conn, packet, sender):
    """Nhan dung so byte file va luu vao thu muc downloads."""
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    filename = os.path.basename(packet["filename"]) or "file"
    target = DOWNLOAD_DIR / filename
    if target.exists():
        target = DOWNLOAD_DIR / f"{target.stem}_{int(time.time())}{target.suffix}"
    # size la tong so byte file ben gui da thong bao.
    remaining = int(packet["size"])
    with open(target, "wb") as f:
        while remaining > 0:
            chunk = conn.recv(min(65536, remaining))
            if not chunk:
                raise ConnectionError("Truyền file bị ngắt")
            f.write(chunk)
            remaining -= len(chunk)
    log(f"[*] Đã nhận file {target} từ {sender}")


def handle_connection(conn, name=None):
    """
    Chạy trong một thread riêng cho mỗi kết nối TCP.
    name=None : người khác kết nối tới ta -> đọc gói hello để biết tên họ.
    name có sẵn: ta chủ động kết nối -> đã biết tên (bên kia không gửi hello lại).
    Gói hello có "channel": "chat" (chat chính) hoặc "file" (kết nối riêng gửi 1 file).
    """
    is_chat = True
    try:
        if name is None:
            hello = recv_json(conn)
            name = hello["name"]
            is_chat = hello.get("channel") == "chat"
            if is_chat:  
                with lock:
                    peer_sockets[name] = conn
                    histories.setdefault(name, [])
                    unread.setdefault(name, 0)
                log(f"[*] {name} vừa kết nối chat với bạn")

        while True:
            packet = recv_json(conn)
            if packet["type"] == "MSG":
                line = f"[{name}] {packet['text']}"
                with lock:
                    histories.setdefault(name, []).append(line)
                    show_now = current["name"] == name
                    if not show_now:
                        unread[name] = unread.get(name, 0) + 1
                if show_now:
                    log(line)
                else:
                    log(f"[*] Tin nhắn mới từ {name} ({unread[name]} chưa đọc)")
            elif packet["type"] == "FILE":
                receive_file(conn, packet, name)
                if not is_chat:
                    break
            elif packet["type"] == "BYE":
                break
    except (OSError, ValueError, KeyError) as e:
        if not is_chat:
            log(f"[!] Nhận file thất bại: {e}")
    finally:
        with lock:
            was_open = peer_sockets.get(name) is conn
            if was_open:
                del peer_sockets[name]
        conn.close()
        if was_open: 
            log(f"[*] Kết nối chat với {name} đã đóng")


def accept_loop(server):
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()


def peer_list():
    with lock:
        return sorted(peers.values(), key=lambda p: p["name"].lower())


def connect_to(name):
    with lock:
        peer = peers.get(name)
        sock = peer_sockets.get(name)
    if not peer:
        log(f"[!] Không tìm thấy {name}. Dùng /list để xem.")
        return
    if not sock:  # chưa có kết nối thì tạo mới
        try:
            sock = socket.create_connection((peer["ip"], peer["port"]), timeout=4)
            sock.settimeout(None)
            send_json(sock, {"name": me["name"], "channel": "chat"})
        except OSError as e:
            log(f"[!] Không kết nối được tới {name}: {e}")
            return
        with lock:
            peer_sockets[name] = sock
        threading.Thread(target=handle_connection, args=(sock, name), daemon=True).start()
    with lock:
        current["name"] = name
        unread[name] = 0
        history = list(histories.setdefault(name, []))
    log("\n".join([f"[*] Đang chat với {name}"] + history))


def send_message(text):
    with lock:
        name = current["name"]
        sock = peer_sockets.get(name)
    if not sock:
        log("[!] Chưa chat với ai (hoặc kết nối đã đóng). Dùng /chat <tên>.")
        return
    try:
        send_json(sock, {"type": "MSG",
                         "text": text})
    except OSError as e:
        log(f"[!] Gửi tin nhắn thất bại: {e}")
        return
    with lock:
        histories.setdefault(name, []).append(f"[Bạn] {text}")
    log(f"[Bạn] {text}")


def send_file(path_text):
    path = Path(path_text).expanduser()
    with lock:
        peer = peers.get(current["name"])
    if not path.is_file():
        log(f"[!] Không tìm thấy file: {path}")
        return
    if not peer:
        log("[!] Hãy /chat với người nhận trước.")
        return

    def worker():
        try:
            with socket.create_connection((peer["ip"], peer["port"]), timeout=5) as s:
                s.settimeout(None)
                send_json(s, {"name": me["name"], 
                              "channel": "file"})
                send_json(s, {"type": "FILE",
                              "filename": path.name,
                              "size": path.stat().st_size})
                total = path.stat().st_size
                sent, last = 0, 0
                with open(path, "rb") as f:
                    while chunk := f.read(65536):
                        s.sendall(chunk)
                        sent += len(chunk)
                        pct = sent * 100 // max(total, 1)
                        if pct >= last + 10:     
                            last = pct
                            log(f"[*] Gửi {path.name}: {pct}%")
                log(f"[*] Đã gửi xong {path.name}")
        except OSError as e:
            log(f"[!] Gửi file thất bại: {e}")

    log(f"[*] Đang gửi {path.name} tới {peer['name']}...")
    threading.Thread(target=worker, daemon=True).start()

HELP = """Lệnh:
  /list                  Xem người dùng trên LAN
  /chat <tên|số>         Bắt đầu/chuyển cuộc chat
  <nội dung>             Gửi tin nhắn cho người đang chat
  /send <đường_dẫn_file> Gửi file cho người đang chat
  /leave                 Đóng cuộc chat hiện tại
  /status <online|away>  Đổi trạng thái
  /help                  Xem trợ giúp
  /quit                  Thoát"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    if not args.name.strip() or "|" in args.name:
        parser.error("--name không được rỗng hoặc chứa ký tự '|'")
    me.update(name=args.name.strip(), port=args.port)


    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("", me["port"]))
    except (OSError, OverflowError) as e:
        parser.exit(1, f"Không mở được cổng TCP {me['port']}: {e}\n")
    server.listen()

    for target, a in ((accept_loop, (server,)), (udp_listener, ()), (udp_broadcaster, ())):
        threading.Thread(target=target, args=a, daemon=True).start()
    log(f"[*] LAN Chat khởi động: {me['name']} (TCP {me['port']}, UDP {UDP_PORT})\n{HELP}")

    session = PromptSession()
    while True:
        try:
            with patch_stdout():
                line = session.prompt("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        if not line.startswith("/"):
            send_message(line)
            continue

        cmd, _, arg = line.partition(" ")
        cmd, arg = cmd.lower(), arg.strip()
        if cmd == "/list":
            items = peer_list()
            if not items:
                log("Chưa thấy ai trên LAN.")
            else:
                log("\n".join(f"  {i}. {p['name']} ({p['ip']}:{p['port']}) — {p['status']}" for i, p in enumerate(items, 1)))
        elif cmd == "/chat":
            items = peer_list()
            if arg.isdigit() and 1 <= int(arg) <= len(items):
                arg = items[int(arg) - 1]["name"]
            if arg:
                connect_to(arg)
            else:
                log("Cách dùng: /chat <tên|số>")
        elif cmd == "/send":
            if arg:
                send_file(arg)
            else:
                log("Cách dùng: /send <đường_dẫn_file>")
        elif cmd == "/leave":
            with lock:
                name, current["name"] = current["name"], None
                sock = peer_sockets.pop(name, None) 
            if sock:
                try:
                    send_json(sock, {"type": "BYE"})
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            log("[*] Đã rời cuộc chat.")
        elif cmd == "/status":
            if arg in ("online", "away"):
                me["status"] = arg
                log(f"[*] Trạng thái: {arg}")
            else:
                log("Cách dùng: /status <online|away>")
        elif cmd == "/help":
            log(HELP)
        elif cmd == "/quit":
            break
        else:
            log("[!] Lệnh không hợp lệ. Gõ /help.")
    log("[*] Đã thoát LAN Chat.")

if __name__ == "__main__":
    main()
