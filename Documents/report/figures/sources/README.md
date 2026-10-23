Nguồn TikZ chỉnh sửa được của các sơ đồ trong báo cáo.

## Sơ đồ triển khai theo bố cục L1 (Hình 3.2)

ch3-deployment-l1.tex là tài liệu standalone hoàn chỉnh. Biên dịch bằng XeLaTeX, với các font DejaVu Sans và DejaVu Sans Mono:

    xelatex -interaction=nonstopmode -halt-on-error ch3-deployment-l1.tex

Chép PDF kết quả vào figures/ch3-deployment-l1.pdf. Wrapper figures/ch3-architecture.tex gọi PDF này. Bố cục tham khảo Hình 1 của Kuzlu và cộng sự (2022), DOI: 10.1109/ISGT50606.2022.9903049; nội dung được vẽ theo triển khai Docker của nền tảng.
Ảnh tổng quan do nhóm cung cấp được lưu tại figures/ch3-overview-author.png và
được chèn trực tiếp thành Hình 3.1 (sơ đồ luồng dữ liệu tổng quát). Nguồn drawio
gốc là Bigdata.drawio, trang Page-1; ảnh là bản xuất PNG của trang đó, không có
mã TeX tương ứng.

## Các sơ đồ còn lại

Biên dịch phần tikzpicture trong wrapper standalone với inputenc UTF-8, fontenc T5, babel vietnamese, tikz, amsmath, setspace và các thư viện shapes, arrows.meta, positioning, fit, backgrounds. Dùng font 12pt và setstretch 1.
Chép PDF mới vào figures/<tên>.pdf. Caption và label của báo cáo nằm ở wrapper figures/*.tex hoặc chapters/*.tex.
Không cần thêm package vào main.tex để dùng các PDF.

ch1-rq-map.tex đã được cập nhật theo phạm vi phân tích dữ liệu IoT đã thu thập.
ch3-architecture.tex là nguồn sơ đồ cũ, được giữ để tham khảo; báo cáo hiện dùng ch3-deployment-l1.pdf.

## Cập nhật căn chỉnh mũi tên (2026-10-21)

Sáu sơ đồ sau đã chuyển từ tọa độ tuyệt đối sang vị trí tương đối (positioning) để mũi tên luôn bám cạnh hộp: ch2-mqtt-ack, ch2-time-taxonomy, ch2-validation-levels, ch2-partition-concurrency, ch3-event-classification, ch3-q3-transaction.
Các wrapper figures/<tên>.tex của sáu sơ đồ này nay chứa trực tiếp mã TikZ (main.tex đã nạp đủ thư viện), nên không cần biên dịch lại PDF tương ứng; các PDF cũ không còn được báo cáo dùng.
