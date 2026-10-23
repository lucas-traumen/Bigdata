# Báo cáo Big Data — phân tích dữ liệu IoT đã thu thập

Bản cập nhật ngày 10/10/2026 từ nguồn Overleaf được cung cấp.
Biên dịch bằng pdfLaTeX, main document: main.tex. Không cần shell escape.
Chạy latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex.

Định dạng trang và trang bìa dùng `hcmutethesis.sty` (HCMUTE): khung viền kép,
logo trường, bảng GVHD/SVTH. Hai trang bìa được sinh tự động bởi `\coverpage`;
thông tin trường/khoa/bộ môn/đề tài/GVHD/SVTH khai báo trong `main.tex`, không
sửa trực tiếp trong `.sty`. File `chapters/titlepage.tex` cũ đã bị xoá.

Khoá bìa trong `main.tex`:
`\crname` (loại báo cáo), `\ctname` (tên đề tài), `\cmjname` (môn học/ngành),
`\cstuname` (bảng sinh viên), `\csSupervise` (GVHD), `\cttime` (thời điểm),
`\csfacultyname` và `\csdeptname` (khoa, bộ môn).

Nội dung có năm chương; kết quả dựa trên dữ liệu IoT được thu thập trong phiên thực nghiệm bốn giờ. Tên lưu trữ do người thực hiện quy ước được ghi trong EVIDENCE_NOTES.md để truy vết nguồn.
Tóm tắt kiểm chứng và truy vấn nằm trong evidence/. Phạm vi đọc trực tiếp và đối chiếu snapshot được ghi trong EVIDENCE_NOTES.md.

Các sơ đồ và biểu đồ trong bản báo cáo dùng PDF vector để giảm thời gian biên dịch Overleaf.
Nguồn TikZ chỉnh sửa được lưu tại figures/sources/. Các chart dữ liệu được tạo từ evidence/verification_summary.json, không phải ảnh minh họa sinh tự động.
Các tệp TeX chart cũ không còn được gọi bởi báo cáo.
Trang bìa dùng thông tin HCMUTE (khoa, bộ môn, GVHD) và bốn dòng sinh viên
để trống tên/MSSV chờ điền. Bìa gồm hai trang: bìa chính có khung viền kép và
bìa lót không viền, theo mẫu `hcmutethesis`.

Hình 3.1 là sơ đồ luồng dữ liệu tổng quát do nhóm vẽ ban đầu, dùng ảnh
figures/ch3-overview-author.png (nguồn drawio gốc: Bigdata.drawio, trang Page-1).
Hình 3.2 được vẽ lại theo bố cục Hình 1 của tài liệu L1, với các thành phần và kết nối của triển khai Docker Compose thực tế: một Mosquitto, một broker Kafka KRaft chứa ba partition, Spark, Parquet cục bộ, PostgreSQL, FastAPI và Nginx. Nguồn L1 được dẫn trong references.bib.
Nguồn chỉnh sửa Hình 3.2 là figures/sources/ch3-deployment-l1.tex, biên dịch độc lập bằng XeLaTeX; hướng dẫn nằm trong figures/sources/README.md. Báo cáo main.tex vẫn dùng pdfLaTeX.

Phụ lục A (`chapters/appendix-deployment.tex`) hướng dẫn triển khai bằng Docker Hub:
clone repo, pull năm image `trilucas/big_data`, gắn tag cục bộ, cấu hình `.env`,
khởi tạo PostgreSQL/Kafka, chạy pipeline và kiểm chứng. Phụ lục không trích image ID
cố định mà dùng cặp tag + digest (Mục A.2) vì image ID đổi mỗi lần build lại.
Phụ lục đặt sau thư mục tham khảo, tiêu đề "Phụ lục A" nhờ `\appendix` +
`\chaptertitlename` trong `main.tex`.

Phạm vi nghiên cứu là dữ liệu IoT đã thu thập và log vận hành của phiên chạy; các số liệu kết quả được giữ nguyên.
