import json
import os
import random
import time
from datetime import datetime, timezone

os.makedirs("events", exist_ok=True)    # crée le dossier events s'il n'existe pas

cities = [      # liste des villes
    "Paris",    
    "Lyon",
    "Marseille",
    "Lille",
    "Bordeaux"
]

categories = [      # Liste des catégories
    "Vehicules",
    "Immobilier",
    "Informatique",
    "Maison",
    "Loisirs"
]

actions = [     # Liste des actions
    "AIME",
    "VEUT_ACHETER",
    "ACHAT"
]

while True:
    product_num = random.randint(1, 50)

    event = {
        "timestamp": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),   # heure UTC sans fuseau (utcnow() est déprécié)
        "user_id": f"usr_{random.randint(1,100)}",
        "user_city": random.choice(cities),
        "product_id": f"prod_{product_num}",
        "product_cat": random.choice(categories),
        "seller_id": f"sel_{(product_num % 20) + 1}",
        "action_type": random.choice(actions),
        "price": round(random.uniform(10, 5000),2)    # round(.,2) permet d'arrondir le nombre à deux chiffres après la virgule
    }

    filename = f"events/event_{int(time.time()*1000)}.json"  # créé les fichiers json dans le fichier events

    with open(filename, "w") as f:
        json.dump(event, f)         # convertit event qui est un dictionnaire à la base en JSON puis l'écrit dans le fichier correspondant

    print(event)

    time.sleep(1)       # on met une pause de 1 seconde entre chaque event
