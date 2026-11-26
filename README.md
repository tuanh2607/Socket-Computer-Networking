# LAN Chat

## How to install

Yêu cầu Python 3.10 trở lên.

```bash
python -m pip install prompt_toolkit
```

## How to run

Mở hai cửa sổ Terminal tại thư mục chứa `lanchat.py` và chạy mỗi phiên với tên, cổng TCP khác nhau:

```bash
python lanchat.py --name Minh --port 5001
python lanchat.py --name Quan --port 5002
```

Hai máy phải ở cùng mạng LAN/Wi-Fi. Nếu chạy trên hai máy, hãy cho phép Python qua firewall. Chương trình dùng UDP port `9999` để tìm kiếm các phiên chat và dùng cổng TCP đã chỉ định để chat.

Sau khi khởi động vài giây:

```text
/list
/chat Quan
Xin chào Quan!
/send sample.txt
```

Các lệnh chính: `/list`, `/chat <tên|số>`, `/send <đường_dẫn_file>`, `/leave`, `/status <online|away>`, `/help`, `/quit`.

File nhận được sẽ được lưu trong thư mục `downloads/`.

## How to test

1. Chạy hai phiên theo hướng dẫn ở trên.
2. Trong một phiên, chạy `/list` và xác nhận phiên còn lại xuất hiện.
3. Dùng `/chat <tên>` rồi gửi một tin nhắn; xác nhận tin nhắn xuất hiện ở phiên nhận.
4. Dùng `/send sample.txt`; xác nhận file được tạo trong `downloads/`.
5. Chạy `/status away` ở một phiên và dùng `/list` ở phiên kia để kiểm tra trạng thái.
6. Thoát một phiên bằng `/quit`, chờ khoảng 10 giây rồi chạy `/list` để kiểm tra phiên đã offline.

