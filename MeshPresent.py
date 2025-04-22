import pandas as pd
import igraph as ig
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # Needed for 3D projection

def plot_gossip_mesh_igraph_chunked_3d(
    csv_file, 
    output_prefix="mesh_t_",
    chunk_size=500_000
):
    """
    Reads a LARGE csv_file in chunks (assuming sorted by Time),
    builds an iGraph for each time, creates a 3D layout,
    and renders a static 3D scatter plot with matplotlib.

    CSV columns expected: Time,Topic,SourceNode,TargetNode
    """

    current_time = None
    edges_for_time = []  # list of (source, target) for the current time

    def build_and_plot_igraph_3d(time_value, edge_list):
        """
        Build an undirected iGraph from `edge_list = [(src, dst), ...]`
        Then compute a 3D layout and render a 3D scatter plot.
        """
        if not edge_list:
            return  # No edges => nothing to plot

        # 1) Collect all unique nodes
        unique_nodes = set()
        for (src, dst) in edge_list:
            unique_nodes.add(src)
            unique_nodes.add(dst)

        node_list = list(unique_nodes)
        # Map each node to an integer index
        node_index = {n: i for i, n in enumerate(node_list)}

        # 2) Convert (src, dst) -> edge index pairs
        ig_edges = []
        for (src, dst) in edge_list:
            ig_edges.append((node_index[src], node_index[dst]))

        # 3) Build an undirected iGraph
        g = ig.Graph(n=len(node_list), edges=ig_edges, directed=False)

        # (Optional) color certain vertices, e.g. block proposer "0" in green
        colors = ["red"] * len(node_list)
        if "0" in node_index:
            colors[node_index["0"]] = "green"
        g.vs["color"] = colors

        # 4) Compute a 3D layout using Fruchterman-Reingold
        #    or Kamada-Kawai:
        #    layout_3d = g.layout_kamada_kawai(dim=3)
        layout_3d = g.layout("fr", dim=3)  # 3D FR layout
        coords_3d = layout_3d.coords  # list of (x, y, z) for each vertex

        # 5) Prepare 3D scatter with matplotlib
        fig = plt.figure(figsize=(8, 6))
        ax = fig.add_subplot(111, projection="3d")

        # Convert to separate X, Y, Z arrays
        xs = [p[0] for p in coords_3d]
        ys = [p[1] for p in coords_3d]
        zs = [p[2] for p in coords_3d]

        # Draw nodes
        ax.scatter(xs, ys, zs, c=colors, s=10, alpha=0.9)

        # Draw edges (as line segments)
        edge_list = g.get_edgelist()
        for (src_idx, dst_idx) in edge_list:
            x_edge = [xs[src_idx], xs[dst_idx]]
            y_edge = [ys[src_idx], ys[dst_idx]]
            z_edge = [zs[src_idx], zs[dst_idx]]
            ax.plot(x_edge, y_edge, z_edge, color="gray", alpha=0.5, linewidth=1)

        ax.set_title(f"GossipSub Mesh at time={time_value} (3D)")
        # Hide axis ticks for a cleaner look
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_zticks([])

        plt.tight_layout()
        outname = f"{output_prefix}{time_value}.png"
        plt.savefig(outname, dpi=300)
        plt.close()
        print(f"Saved {outname}")

    # ----------------------------------------------------------------
    # Chunked reading of the CSV
    # ----------------------------------------------------------------
    for chunk_idx, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
        # Expect columns: Time,Topic,SourceNode,TargetNode
        for row in chunk.itertuples(index=False):
            row_time = row.Time
            src = row.SourceNode
            dst = row.TargetNode

            if current_time is None:
                # First row
                current_time = row_time

            if row_time != current_time:
                # We finished collecting edges for 'current_time'
                build_and_plot_igraph_3d(current_time, edges_for_time)
                # Reset
                current_time = row_time
                edges_for_time = []

            edges_for_time.append((src, dst))

    # Plot final time after reading all chunks
    if current_time is not None and edges_for_time:
        build_and_plot_igraph_3d(current_time, edges_for_time)


if __name__ == "__main__":
    # Example usage. If your CSV is large and unsorted, 
    # sort it by Time first or you'll mix up times within a chunk.
    plot_gossip_mesh_igraph_chunked_3d(
        csv_file="mesh_connections.csv",
        output_prefix="mesh_t_3d_",
        chunk_size=500_000
    )

