# LAN Chat & File Sharing — Demo starter

Đây là bản mẫu chạy được để bạn hình dung project Socket Programming. Bản này minh họa UDP discovery/status, TCP chat, gửi file, thread và các lệnh CLI. Nó là starter để học và phát triển tiếp, chưa phải bản nộp hoàn chỉnh.

## 1. Yêu cầu
- Python 3.10 trở lên
- Không cần cài thư viện ngoài
- Hai máy cùng mạng LAN/Wi-Fi; hoặc thử nhiều phiên bản trên một máy
- Có thể cần cho phép Python qua Windows Firewall

## 2. Chạy thử trên một máy tính
Mở 2 cửa sổ Terminal/CMD tại thư mục chứa `lanchat.py`.

Cửa sổ 1:
```bash
python lanchat.py --name Minh --port 5001
```

Cửa sổ 2:
```bash
python lanchat.py --name Quan --port 5002
```

Chờ khoảng 3–6 giây, sau đó nhập `/list` ở một cửa sổ. Nếu discovery UDP loopback/broadcast bị chặn bởi hệ điều hành hoặc firewall, hãy thử trên hai máy cùng Wi-Fi hoặc cấu hình firewall cho Python.

Ở cửa sổ Quan:
```text
/chat Minh
Xin chào Minh!
/send sample.txt
```
Ở cửa sổ Minh, bạn sẽ thấy tin nhắn và file được lưu trong `downloads/`.

## 3. Chạy trên hai máy
1. Kết nối hai máy vào cùng mạng LAN/Wi-Fi.
2. Xem IP LAN của mỗi máy (`ipconfig` trên Windows, `ip addr` trên Linux).
3. Chạy chương trình với tên và cổng TCP khác nhau, ví dụ:
   - Máy A: `python lanchat.py --name Minh --port 5001`
   - Máy B: `python lanchat.py --name Quan --port 5002`
4. Cho phép Python qua firewall; UDP discovery dùng cổng 9999, TCP dùng cổng bạn chỉ định.
5. Chạy `/list`, rồi `/chat Quan` hoặc `/chat Minh`.

## 4. Lệnh
- `/list`
- `/chat <tên|số>`
- Gõ nội dung bình thường để gửi tin nhắn
- `/send <đường_dẫn_file>`
- `/leave`
- `/status <online|away>`
- `/help`
- `/quit`

## 5. Cấu trúc hoạt động
- UDP broadcaster: gửi `HELLO|name|tcp_port|status` mỗi 3 giây.
- UDP listener: cập nhật peer và timeout sau 10 giây.
- TCP: JSON packet có header độ dài 4 byte, theo sau là JSON UTF-8. File có header metadata rồi truyền đúng số byte thô.
- Thread: broadcaster, listener, TCP acceptor, receiver theo kết nối và file sender.

## 6. Giới hạn cần biết
- Bản demo giữ giao thức đơn giản để dễ đọc; không có mã hóa/xác thực.
- Màn hình CLI dùng `input()` cơ bản, vì vậy chưa bảo đảm yêu cầu nâng cao “đang gõ mà tin nhắn đến vẫn giữ nguyên dòng nhập”. Cần bổ sung `prompt_toolkit` hoặc tự vẽ lại input để đạt yêu cầu đó.
- Có thể cần điều chỉnh UDP broadcast tùy hệ điều hành/router/firewall.
- Nếu cùng một người dùng kết nối lại hoặc nhiều lần kết nối, cần cải thiện quản lý socket/session.
- Chưa có kiểm thử tự động đầy đủ.

## 7. Checklist demo
- [ ] Mở 2 phiên bản, thấy nhau trong `/list`
- [ ] `/status away`, kiểm tra trạng thái cập nhật
- [ ] `/chat <tên>`, chat hai chiều
- [ ] Gửi file bằng `/send sample.txt`
- [ ] Xác nhận file nằm trong `downloads/`
- [ ] Thử tắt một phiên bản và quan sát timeout/offline
- [ ] Chụp Wireshark: UDP port 9999 và TCP handshake
