from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
import numpy as np

# du lieu gia dinh: [so gio hoc/tuan, ti le diem danh] -> dau (1) / rot (0)
X = np.array([[2,60],[3,70],[8,95],[1,50],[9,98],[4,75],[7,90],[2,55]])
y = np.array([0,0,1,0,1,0,1,0])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=1)
clf = DecisionTreeClassifier(max_depth=3, random_state=1)
clf.fit(X_train, y_train)

print("Do chinh xac tren tap test:", clf.score(X_test, y_test))
print("Du doan cho [6 gio hoc, 85% diem danh]:", clf.predict([[6, 85]]))
