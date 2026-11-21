#!/usr/bin/env python3
"""
LAN Chat & File Sharing (bản gọn).
- UDP broadcast: các máy báo "tôi đang online" để tìm thấy nhau.
- TCP: gửi tin nhắn và file.
Chạy: python lanchatV2.py --name An --port 5001
"""
import argparse              # Doc tham so tu command line.
import json                  # Ma hoa/giai ma du lieu JSON.
import os                    # Xu ly ten file an toan.
import socket                # Tao ket noi TCP/UDP.
import struct                # Dong goi header do dai goi tin TCP.
import threading             # Chay nhieu tac vu dong thoi.
import time                  # Tao khoang nghi va ghi nhan thoi gian.
from pathlib import Path     # Xu ly duong dan va file.

# PromptSession quan ly dong nhap lenh; patch_stdout ve lai prompt khi co print().
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout

# Cong UDP dung de phat hien cac ung dung dang online.
UDP_PORT = 9999
# Thu muc luu file nhan duoc tu nguoi dung khac.
DOWNLOAD_DIR = Path("downloads")

# Bang thong tin peer duoc cap nhat boi UDP listener.
peers = {}         # tên -> {name, ip, port, status, last_seen}
# Socket TCP chat dang mo voi tung peer.
peer_sockets = {}  # tên -> socket chat đang mở
# Lich su tin nhan theo ten peer.
histories = {}     # tên -> danh sách tin nhắn
# So tin nhan chua doc theo tung peer.
unread = {}        # tên -> số tin chưa đọc
# Ten peer dang duoc chon de gui tin nhan.
current = {"name": None}  # người đang chat
# Thong tin cua tien trinh hien tai.
me = {"name": "", "port": 0, "status": "online"}
# Khoa dung chung de tranh hai thread sua du lieu cung luc. Reentrantlock
lock = threading.RLock()


def log(message):
    """In thong bao ra man hinh va flush ngay de hien thi tuc thi."""
    print(f"\n{message}", flush=True)


# ---------- Gói tin TCP: [4 byte độ dài][JSON] ----------
def send_json(sock, obj):
    """Gui object JSON theo dinh dang [4 byte do dai][payload]."""
    # Chuyen object Python thanh chuoi JSON roi ma hoa thanh UTF-8.
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    # Gui header truoc de ben nhan biet chinh xac kich thuoc payload.
    sock.sendall(struct.pack("!I", len(data)) + data)


def recv_exact(sock, n):
    """Doc dung n byte vi TCP co the tra ve it byte hon yeu cau."""
    buf = b""
    while len(buf) < n:
        # Lap lai den khi nhan du so byte can thiet.
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Kết nối đã đóng")
        buf += chunk
    return buf


def recv_json(sock):
    """Doc header, doc payload va giai ma payload thanh dictionary."""
    # Header cho biet payload dai bao nhieu byte.
    size = struct.unpack("!I", recv_exact(sock, 4))[0]
    # Gioi han kich thuoc de tranh goi tin bat thuong.
    if size > 64 * 1024:
        raise ValueError("Gói tin quá lớn")
    # Doc dung kich thuoc payload va chuyen JSON ve object Python.
    obj = json.loads(recv_exact(sock, size))
    if not isinstance(obj, dict):
        raise ValueError("Gói tin không hợp lệ")
    return obj


# ---------- UDP: tìm người dùng ----------
def udp_broadcaster():
    """Dinh ky broadcast thong tin cua minh de cac may khac tim thay."""
    # Tao socket UDP; SO_BROADCAST cho phep gui den dia chi broadcast.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    while True:
        # Dinh dang don gian: HELLO|ten|TCP port|trang thai.
        msg = f"HELLO|{me['name']}|{me['port']}|{me['status']}".encode()
        for addr in ("255.255.255.255", "127.255.255.255"):  # LAN + cùng máy
            try:
                # Gui cho LAN va loopback de test nhieu cua so tren cung may.
                sock.sendto(msg, (addr, UDP_PORT))
            except OSError:
                pass
        time.sleep(3)


def udp_listener():
    """Lang nghe broadcast, cap nhat danh sach peers va loai peer offline."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    # Bind vao moi interface tren may tai cong UDP da quy uoc.
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
                    peers[name] = {"name": name, "ip": addr[0], "port": int(port),
                                   "status": status, "last_seen": time.time()}
                if is_new:
                    log(f"[*] {name} đã online")
        except (socket.timeout, ValueError):
            pass
        # Peer nào quá 10 giây không báo thì coi là offline
        with lock:
            for name in [n for n, p in peers.items() if time.time() - p["last_seen"] > 10]:
                del peers[name]
                log(f"[*] {name} đã offline")


# ---------- TCP: nhận dữ liệu ----------
def receive_file(conn, packet, sender):
    """Nhan dung so byte file va luu vao thu muc downloads."""
    # Tao thu muc neu chua ton tai; exist_ok giup khong bi loi neu da co.
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    filename = os.path.basename(packet["filename"]) or "file"  # chống tên kiểu ../../x
    target = DOWNLOAD_DIR / filename
    if target.exists():  # không ghi đè file cũ
        target = DOWNLOAD_DIR / f"{target.stem}_{int(time.time())}{target.suffix}"
    # size la tong so byte file ben gui da thong bao.
    remaining = int(packet["size"])
    with open(target, "wb") as f:
        while remaining > 0:
            # Moi lan doc toi da 64 KB, sau do giam so byte con lai.
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
    # True neu day la ket noi chat; False neu day la ket noi gui file rieng.
    is_chat = True
    try:
        if name is None:
            # Ket noi den tu peer khac: doc goi HELLO de biet ten nguoi gui.
            hello = recv_json(conn)
            name = hello["name"]
            is_chat = hello.get("channel") == "chat"
            if is_chat:  # kênh file không được ghi đè socket chat
                # Chi luu socket chat chinh; socket file khong ghi de len no.
                with lock:
                    peer_sockets[name] = conn
                    histories.setdefault(name, [])
                    unread.setdefault(name, 0)
                log(f"[*] {name} vừa kết nối chat với bạn")

        while True:
            # Moi vong lap doc mot goi tin JSON tu ket noi TCP.
            packet = recv_json(conn)
            if packet["type"] == "MSG":
                # Luu lich su truoc khi hien thi de co the xem lai bang /chat.
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
                # Goi FILE di kem metadata, sau metadata la bytes noi dung file.
                receive_file(conn, packet, name)
                if not is_chat:
                    break
            elif packet["type"] == "BYE":
                # Peer yeu cau dong ket noi chat.
                break
    except (OSError, ValueError, KeyError) as e:
        if not is_chat:
            log(f"[!] Nhận file thất bại: {e}")
    finally:
        with lock:
            was_open = peer_sockets.get(name) is conn
            if was_open:
                del peer_sockets[name]
        # Giai phong socket sau khi ket thuc ket noi hoac gap loi.
        conn.close()
        if was_open:  # nếu tự /leave thì socket đã bị gỡ trước, không báo
            log(f"[*] Kết nối chat với {name} đã đóng")


def accept_loop(server):
    """Nhan ket noi TCP moi va tao thread rieng cho moi ket noi."""
    while True:
        # accept() cho den khi co client ket noi vao server socket.
        conn, _ = server.accept()
        # Thread rieng giup server van nhan client moi khi dang chat.
        threading.Thread(target=handle_connection, args=(conn,), daemon=True).start()


# ---------- Các lệnh của người dùng ----------
def peer_list():
    """Tra ve danh sach peer duoc sap xep theo ten de hien thi."""
    with lock:
        return sorted(peers.values(), key=lambda p: p["name"].lower())


def connect_to(name):
    """Tao hoac tai su dung ket noi chat den peer co ten duoc chon."""
    with lock:
        peer = peers.get(name)
        sock = peer_sockets.get(name)
    # Peer phai ton tai trong danh sach do UDP discovery cap nhat.
    if not peer:
        log(f"[!] Không tìm thấy {name}. Dùng /list để xem.")
        return
    if not sock:  # chưa có kết nối thì tạo mới
        try:
            # Ket noi den IP va TCP port ma peer da broadcast.
            sock = socket.create_connection((peer["ip"], peer["port"]), timeout=4)
            sock.settimeout(None)
            # Gui HELLO de peer biet ten va day la kenh chat.
            send_json(sock, {"name": me["name"], "channel": "chat"})
        except OSError as e:
            log(f"[!] Không kết nối được tới {name}: {e}")
            return
        with lock:
            peer_sockets[name] = sock
        # Thread nay lang nghe tin nhan den trong khi main cho lenh nguoi dung.
        threading.Thread(target=handle_connection, args=(sock, name), daemon=True).start()
    with lock:
        current["name"] = name
        unread[name] = 0
        history = list(histories.setdefault(name, []))
    log("\n".join([f"[*] Đang chat với {name}"] + history))


def send_message(text):
    """Gui text den peer dang chat va luu tin nhan vao lich su."""
    with lock:
        name = current["name"]
        sock = peer_sockets.get(name)
    if not sock:
        log("[!] Chưa chat với ai (hoặc kết nối đã đóng). Dùng /chat <tên>.")
        return
    try:
        # Gui message theo cung giao thuc JSON-framed voi ben nhan.
        send_json(sock, {"type": "MSG", "text": text})
    except OSError as e:
        log(f"[!] Gửi tin nhắn thất bại: {e}")
        return
    with lock:
        histories.setdefault(name, []).append(f"[Bạn] {text}")
    log(f"[Bạn] {text}")


def send_file(path_text):
    """Gui file bang mot ket noi TCP rieng de khong chan kenh chat."""
    path = Path(path_text).expanduser()
    with lock:
        peer = peers.get(current["name"])
    if not path.is_file():
        log(f"[!] Không tìm thấy file: {path}")
        return
    if not peer:
        log("[!] Hãy /chat với người nhận trước.")
        return

    def worker():  # kết nối TCP riêng để chat không bị chặn khi gửi file lớn
        try:
            # Mo TCP connection rieng, chi dung cho mot lan gui file.
            with socket.create_connection((peer["ip"], peer["port"]), timeout=5) as s:
                s.settimeout(None)
                # Bao cho ben nhan biet day la kenh file, khong phai chat.
                send_json(s, {"name": me["name"], "channel": "file"})
                send_json(s, {"type": "FILE", "filename": path.name,
                              "size": path.stat().st_size})
                total = path.stat().st_size
                sent, last = 0, 0
                with open(path, "rb") as f:
                    # Doc tung block 64 KB va gui het block qua TCP.
                    while chunk := f.read(65536):
                        s.sendall(chunk)
                        sent += len(chunk)
                        pct = sent * 100 // max(total, 1)
                        if pct >= last + 10:          # in mỗi 10% để khỏi spam
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
    """Khoi tao server, cac thread nen va vong lap xu ly lenh CLI."""
    # Tao bo phan tich tham so de moi cua so co ten va TCP port rieng.
    parser = argparse.ArgumentParser(description="LAN Chat & File Sharing")
    parser.add_argument("--name", required=True, help="Tên hiển thị của bạn")
    parser.add_argument("--port", type=int, default=5000, help="Cổng TCP của bản này")
    # Doc cac tham so nguoi dung truyen khi chay file Python.
    args = parser.parse_args()
    if not args.name.strip() or "|" in args.name:
        parser.error("--name không được rỗng hoặc chứa ký tự '|'")
    # Luu thong tin cua tien trinh hien tai de broadcast va ket noi.
    me.update(name=args.name.strip(), port=args.port)




    # Tao socket TCP ma server dung de lang nghe ket noi chat den.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("", me["port"]))
    except (OSError, OverflowError) as e:
        parser.exit(1, f"Không mở được cổng TCP {me['port']}: {e}\n")
    # Chuyen socket sang trang thai listening.
    server.listen()

    # Chay ba tac vu nen: nhan TCP, nghe UDP va phat UDP.
    for target, a in ((accept_loop, (server,)), (udp_listener, ()), (udp_broadcaster, ())):
        threading.Thread(target=target, args=a, daemon=True).start()
    log(f"[*] LAN Chat khởi động: {me['name']} (TCP {me['port']}, UDP {UDP_PORT})\n{HELP}")

    # PromptSession thay input() de tin nhan den khong pha dong dang go.
    session = PromptSession()
    while True:
        try:
            # Giữ dấu nhắc và nội dung đang gõ khi thread khác in tin nhắn.
            # patch_stdout tu dong ve lai prompt sau khi thread khac print().
            with patch_stdout():
                line = session.prompt("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        # Dong khong bat dau bang / duoc xem la noi dung chat.
        if not line.startswith("/"):
            send_message(line)
            continue

        # Tach lenh dau tien va phan tham so con lai.
        cmd, _, arg = line.partition(" ")
        cmd, arg = cmd.lower(), arg.strip()
        if cmd == "/list":
            # Hien thi cac peer ma UDP listener da tim thay.
            items = peer_list()
            if not items:
                log("Chưa thấy ai trên LAN.")
            else:
                log("\n".join(f"  {i}. {p['name']} ({p['ip']}:{p['port']}) — {p['status']}"
                              for i, p in enumerate(items, 1)))
        elif cmd == "/chat":
            # Chon peer theo ten hoac theo so thu tu trong danh sach.
            items = peer_list()
            if arg.isdigit() and 1 <= int(arg) <= len(items):
                arg = items[int(arg) - 1]["name"]  # cho phép gõ số thứ tự
            if arg:
                connect_to(arg)
            else:
                log("Cách dùng: /chat <tên|số>")
        elif cmd == "/send":
            # Gui file bang thread rieng de van nhan duoc tin nhan chat.
            if arg:
                send_file(arg)
            else:
                log("Cách dùng: /send <đường_dẫn_file>")
        elif cmd == "/leave":
            # Dong socket chat hien tai va xoa peer khoi bang socket.
            with lock:
                name, current["name"] = current["name"], None
                sock = peer_sockets.pop(name, None)  # gỡ trước để handler không báo lỗi
            if sock:
                try:
                    send_json(sock, {"type": "BYE"})
                    sock.shutdown(socket.SHUT_RDWR)  # đánh thức thread đang chờ recv
                except OSError:
                    pass
                sock.close()
            log("[*] Đã rời cuộc chat.")
        elif cmd == "/status":
            # Cap nhat status; UDP broadcaster se phat status moi o lan tiep theo.
            if arg in ("online", "away"):
                me["status"] = arg
                log(f"[*] Trạng thái: {arg}")
            else:
                log("Cách dùng: /status <online|away>")
        elif cmd == "/help":
            log(HELP)
        elif cmd == "/quit":
            # Thoat vong lap nhap lenh; cac thread daemon se tu ket thuc.
            break
        else:
            log("[!] Lệnh không hợp lệ. Gõ /help.")

    log("[*] Đã thoát LAN Chat.")


if __name__ == "__main__":
    main()
