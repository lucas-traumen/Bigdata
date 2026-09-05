from sklearn.linear_model import LinearRegression
import numpy as np

# du lieu gia dinh: [chi phi quang cao (trieu VND)] -> doanh thu (trieu VND)
X = np.array([[10],[20],[30],[40],[50],[60]])
y = np.array([120, 210, 300, 405, 500, 590])

model = LinearRegression()
model.fit(X, y)

print(f"beta_0 (intercept) = {model.intercept_:.2f}")
print(f"beta_1 (he so)     = {model.coef_[0]:.2f}")
print("Du doan doanh thu khi chi 45 trieu quang cao:", model.predict([[45]]))
