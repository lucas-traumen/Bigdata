# Báo cáo Big Data — bản bổ sung hình và biểu đồ

Đề tài: **Xây dựng và đánh giá nền tảng Big Data xử lý dữ liệu cảm biến IoT theo thời gian thực**.

Bản này được biên tập từ repository [lucas-traumen/Bigdata](https://github.com/lucas-traumen/Bigdata), commit `6343a180f84c46b611fbef73db1e394fb201abe3` trên nhánh `master`. Giữ nguyên cấu trúc năm chương và bộ package LaTeX; không thay đổi mã nguồn runtime.

Khi đưa bản sửa về repository, thư mục `report/` trong gói nguồn tương ứng với `Documents/report/`. Nên kiểm tra diff trước khi ghi đè nếu repository đã có thay đổi mới hơn commit nguồn.

## Nội dung chỉnh sửa

- Bổ sung hình khái niệm trong chương 1–2, sơ đồ giao dịch PostgreSQL trong chương 3 và ba biểu đồ từ số liệu đã được ghi nhận trong chương 4.
- Vẽ lại hai hình kiến trúc và phân loại event để dễ đọc hơn.
- Thống nhất mô tả validation theo mức row/metric, vị trí khử trùng, timestamp Kafka, cách upsert Gold, cấu hình triển khai và giới hạn scale-out.
- Thay các bảng/đồ thị số giả định chưa kiểm chứng bằng kế hoạch đánh giá. Tách phép tải dài khỏi các fixture kiểm thử trước đó.
- Đồng bộ tóm tắt, kết luận và tài liệu tham khảo với các chỉnh sửa trên.

Hệ thống hỗ trợ sáu chỉ số môi trường. Mỗi bản tin mang tập con tùy loại trạm, không bắt buộc chứa đủ cả sáu.

## Cấu trúc nguồn

- `main.tex`: file chính.
- `chapters/`: bìa, tóm tắt và năm chương.
- `figures/`: nguồn TikZ/pgfplots của các hình. Chương 1–2 include cả môi trường `figure`; chương 3–4 include phần đồ họa bên trong môi trường `figure` của chương.
- `references.bib`, `IEEEtran.bst`: tài liệu tham khảo.
- `EVIDENCE_NOTES.md`: nguồn số liệu, danh mục hình và giới hạn diễn giải.
- `evidence/`: các tài liệu đánh giá và checklist evidence dùng để đối chiếu số liệu. Không chứa log thô của phiên chạy.

## Build PDF

Cần TeX Live có hỗ trợ tiếng Việt (VNTeX, Babel Vietnamese, encoding T5), các package đã khai báo trong `main.tex`, BibTeX và `latexmk`. Trên Ubuntu/Debian, các gói thường dùng là `texlive-latex-extra`, `texlive-lang-other`, `texlive-fonts-recommended` và `latexmk`.

Chạy từ thư mục chứa `main.tex`:

```bash
TEXINPUTS="build:" latexmk -pdf -outdir=build -interaction=nonstopmode -halt-on-error main.tex
```

Kết quả: `build/main.pdf`. Nếu có file `main.aux`, `main.bbl` hoặc `main.blg` của một lần build cũ trong thư mục gốc, chuyển chúng ra ngoài trước khi build để tránh đọc nhầm bibliography.

## Quy ước

- Sơ đồ và biểu đồ là đồ họa vector, chỉnh sửa trực tiếp trong `.tex`; không cần dịch vụ tạo ảnh hay PNG bên ngoài.
- Body của `lstlisting` chỉ dùng ASCII. Tiếng Việt đặt ở caption hoặc đoạn giải thích.
- Caption có citation cần short caption không chứa citation để tránh thay đổi thứ tự đánh số tài liệu qua danh sách hình/bảng.
- Số liệu runtime của phiên chạy dài được đối chiếu từ summary, Bronze summary, stream progress, resource report và health check; lần biên tập này không chạy lại workload.
- Bìa còn các chỗ điền tên trường, sinh viên, MSSV, giảng viên và lớp. Điền thông tin thật trước khi nộp.
