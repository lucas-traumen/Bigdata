from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np

# du lieu gia dinh: [so don hang/thang, gia tri don trung binh]
khach_hang = np.array([
    [2, 150], [3, 180], [1, 120],      # nhom mua it, gia tri thap
    [15, 90], [18, 100], [20, 85],     # nhom mua nhieu, gia tri thap (an vat)
    [4, 900], [5, 1200], [3, 1000],    # nhom mua it, gia tri cao (VIP)
])

# ==== Phan a: K-Means tren du lieu goc (theo slide) ====
model = KMeans(n_clusters=3, random_state=42, n_init=10)
labels = model.fit_predict(khach_hang)

print("[Phan a] Nhan cum:", labels)
print("[Phan a] Tam cum:\n", model.cluster_centers_.round(1))

# ==== Phan b: K-Means sau khi chuan hoa du lieu ====
# Raw features have very different scales: orders per month (1-20) vs
# average order value in VND-like units (85-1200). Euclidean distance is
# dominated by the high-value dimension, so KMeans on raw data splits the
# points mainly by order value instead of the 3 intuitive segments.
# Standardization (zero mean, unit variance per feature) gives both
# features equal weight, so KMeans recovers the 3 expected customer groups.
scaler = StandardScaler()
khach_hang_chuan_hoa = scaler.fit_transform(khach_hang)

model_b = KMeans(n_clusters=3, random_state=42, n_init=10)
labels_b = model_b.fit_predict(khach_hang_chuan_hoa)

print("[Phan b] Nhan cum:", labels_b)
