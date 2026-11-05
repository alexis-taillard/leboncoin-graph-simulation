import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import os
import glob
import time

plt.ion()

vertices_file = "output/vertices"
edges_file = "output/edges"

# couleur des noeuds selon leur type
COULEURS = {
    "USER": "tab:blue",
    "PRODUCT": "tab:green",
    "SELLER": "tab:orange"
}

G = nx.DiGraph()


def lire_csv_spark(dossier):
    # Spark écrit plusieurs fichiers part-*.csv par export (un par partition),
    # certains peuvent ne contenir que l'en-tête : on les lit tous et on les assemble
    fichiers = glob.glob(os.path.join(dossier, "part*.csv"))

    dataframes = [
        pd.read_csv(f)
        for f in fichiers
        if os.path.getsize(f) > 0
    ]

    if len(dataframes) == 0:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


while True:

    try:

        vertices = lire_csv_spark(vertices_file)
        edges = lire_csv_spark(edges_file)

        if vertices.empty and edges.empty:
            print("En attente des données...")
            time.sleep(5)
            continue

        # Ajout progressif des sommets
        for _, row in vertices.iterrows():

            G.add_node(
                row["id"],
                node_type=row["type"]
            )

        # Ajout progressif des arêtes
        for _, row in edges.iterrows():

            if not G.has_edge(
                row["src"],
                row["dst"]
            ):

                G.add_edge(
                    row["src"],
                    row["dst"]
                )

        plt.clf()

        pos = nx.spring_layout(
            G,
            seed=42
        )

        couleurs_noeuds = [
            COULEURS.get(G.nodes[n].get("node_type"), "lightgray")
            for n in G.nodes
        ]

        nx.draw(
            G,
            pos,
            with_labels=True,
            node_size=500,
            node_color=couleurs_noeuds,
            font_size=7
        )

        plt.title(
            f"Graphe dynamique - {len(G.nodes())} noeuds / {len(G.edges())} relations"
        )

        plt.pause(5)

    except Exception as e:

        print(e)
        time.sleep(5)