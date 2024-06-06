import pickle
from sklearn.cluster import KMeans
import numpy as np

# Example training data
data = np.array([
    [1.2, 2.3],
    [3.4, 4.5],
    [5.6, 6.7],
    [7.8, 8.9]
])

# Train KMeans model
kmeans = KMeans(n_clusters=2, random_state=42)
kmeans.fit(data)

# Save the model
with open('models/segmentation_model.pkl', 'wb') as f:
    pickle.dump(kmeans, f)
