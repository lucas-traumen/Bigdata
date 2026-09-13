# Plan: Tuần 2 — Phần lý thuyết (5V + phân loại bài toán)

## Trạng thái
- Approved by user ("làm tiếp assignment", 2026-08-30). Phần môi trường + 6 lab + báo cáo đã xong và được accept ngầm qua bước này.

## Phạm vi
1. Mở rộng `homework/week2/report/main.tex` (báo cáo hiện có, 14 trang) thêm:
   - **Case study 5V**: hệ thống gọi xe/giao đồ ăn — bảng 5V với số liệu minh họa cụ thể,
     tính toán ước lượng rõ ràng (ví dụ Volume: 3 triệu chuyến/ngày × ~400 điểm GPS/chuyến
     ≈ 1,2 tỷ bản ghi/ngày ≈ 14.000 bản ghi/giây trung bình).
   - **Bài nhóm 5V (ví dụ minh họa)**: hệ thống cảm biến IoT nông nghiệp (khớp chủ đề
     báo cáo lớn của repo) — ghi rõ là ví dụ để nhóm chỉnh sửa theo thảo luận thật.
   - **Phân loại 8 bài toán**: bảng 3 cột (kỹ thuật / đặc điểm dữ liệu / mô hình tính toán)
     theo quy trình 3 bước, kèm lập luận ngắn cho từng bài. Nội dung bám đáp án gợi ý
     của slide (trang 136).
   - Cập nhật phần tổng hợp/kết luận nếu cần.
2. Build lại PDF sạch lỗi.

## Ngoài phạm vi
- Code mới (phần này thuần lý thuyết).
- Commit/push.

## Acceptance criteria
- `report/build/main.pdf` build sạch, có đủ 3 phần trên, số liệu 5V có tính toán kiểm chứng được.
