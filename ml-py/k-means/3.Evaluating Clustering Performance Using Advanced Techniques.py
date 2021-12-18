import matplotlib.pyplot as plt
from kneed import KneeLocator
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from sklearn.cluster import DBSCAN
from sklearn.datasets import make_moons
from sklearn.metrics import adjusted_rand_score

# The elbow method and silhouette coefficient evaluate clustering performance without the use of ground truth labels.
# Ground truth labels categorize data points into groups based on assignment by a human or an existing algorithm.
# These types of metrics do their best to suggest the correct number of clusters but can be deceiving when used
# without context.
# Note: In practice, it’s rare to encounter datasets that have ground truth labels.

# When comparing k-means against a density-based approach on nonspherical clusters, the results from the elbow
# method and silhouette coefficient rarely match human intuition. This scenario highlights why advanced clustering
# evaluation techniques are necessary. To visualize an example, import these additional modules:

# This time, use make_moons() to generate synthetic data in the shape of crescents:
features, true_labels = make_moons(
    n_samples=250, noise=0.05, random_state=42
)
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)

# Fit both a k-means and a DBSCAN algorithm to the new data and visually assess the performance by plotting the cluster assignments with Matplotlib:
# Instantiate k-means and dbscan algorithms
kmeans = KMeans(n_clusters=2)
dbscan = DBSCAN(eps=0.3)

# Fit the algorithms to the features
kmeans.fit(scaled_features)
dbscan.fit(scaled_features)

# Compute the silhouette scores for each algorithm
kmeans_silhouette = silhouette_score(
    scaled_features, kmeans.labels_
).round(2)
dbscan_silhouette = silhouette_score(
    scaled_features, dbscan.labels_
).round (2)

# Print the silhouette coefficient for each of the two algorithms and compare them. A higher silhouette coefficient
# suggests better clusters, which is misleading in this scenario:
print('kmeans_silhouette=',kmeans_silhouette)

print('dbscan_silhouette=',dbscan_silhouette)

# This suggests that you need a better method to compare the performance of these two clustering algorithms.
#
# If you’re interested, you can find the code for the above plot by expanding the box below.
# To learn more about plotting with Matplotlib and Python, check out Python Plotting with Matplotlib (Guide). Here’s how you can plot the comparison of the two algorithms in the crescent moons example:
# Plot the data and cluster silhouette comparison
fig, (ax1, ax2) = plt.subplots(
    1, 2, figsize=(8, 6), sharex=True, sharey=True
)
fig.suptitle(f"Clustering Algorithm Comparison: Crescents", fontsize=16)
fte_colors = {
    0: "#008fd5",
    1: "#fc4f30",
}
# The k-means plot
km_colors = [fte_colors[label] for label in kmeans.labels_]
ax1.scatter(scaled_features[:, 0], scaled_features[:, 1], c=km_colors)
ax1.set_title(
    f"k-means\nSilhouette: {kmeans_silhouette}", fontdict={"fontsize": 12}
)

# The dbscan plot
db_colors = [fte_colors[label] for label in dbscan.labels_]
ax2.scatter(scaled_features[:, 0], scaled_features[:, 1], c=db_colors)
ax2.set_title(
    f"DBSCAN\nSilhouette: {dbscan_silhouette}", fontdict={"fontsize": 12}
)
plt.show()

# Since the ground truth labels are known, it’s possible to use a clustering metric that considers labels in its evaluation. You can use the scikit-learn implementation of a common metric called the adjusted rand index (ARI). Unlike the silhouette coefficient, the ARI uses true cluster assignments to measure the similarity between true and predicted labels.
#
# Compare the clustering results of DBSCAN and k-means using ARI as the performance metric:

ari_kmeans = adjusted_rand_score(true_labels, kmeans.labels_)
ari_dbscan = adjusted_rand_score(true_labels, dbscan.labels_)

print('round(ari_kmeans, 2)=',round(ari_kmeans, 2))
print('round(ari_dbscan, 2)=',round(ari_dbscan, 2))





