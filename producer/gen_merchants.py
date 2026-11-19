"""Genere le referentiel statique des commercants SahlPay.

Sortie : data/merchants.json, 300 lignes au format JSON Lines
(une ligne = un commercant), consommable directement par
spark.read.schema(...).json(...) en Partie 3.

    python producer/gen_merchants.py --n 300 --out data/merchants.json
"""
import argparse
import json
import random
from pathlib import Path

# Villes marocaines avec coordonnees approximatives du centre-ville.
CITIES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Fes": (34.0181, -5.0078),
    "Tanger": (35.7595, -5.8340),
    "Agadir": (30.4278, -9.5981),
    "Meknes": (33.8935, -5.5473),
    "Oujda": (34.6814, -1.9086),
    "Kenitra": (34.2610, -6.5802),
    "Tetouan": (35.5785, -5.3684),
}

# Categories de commercants et prefixes d'enseignes plausibles.
CATEGORIES = {
    "epicerie": ["Hanout", "Souk Market", "Epicerie El Baraka", "Mini Marche"],
    "carburant": ["Station Atlas", "Petrom", "Station Al Amane", "Oil Maroc"],
    "telecom": ["Telecom Shop", "Boutique Mobile", "Recharge Express"],
    "restauration": ["Cafe Riad", "Snack Medina", "Restaurant Zitoune", "Chez Karim"],
    "pharmacie": ["Pharmacie Al Shifa", "Pharmacie Centrale", "Pharmacie An Nour"],
    "e-commerce": ["ShopOnline", "MarocStore", "ClickAchat"],
    "transport": ["Taxi Plus", "Bus City", "Transport Rapide"],
    "mode": ["Boutique Nour", "Pret a Porter", "Mode Atlas"],
}


def build_merchants(n: int, seed: int):
    rng = random.Random(seed)
    cities = list(CITIES)
    categories = list(CATEGORIES)
    # Poids : Casablanca et Rabat concentrent davantage de commercants.
    city_weights = [3.0, 2.0, 1.5, 1.2, 1.2, 1.0, 0.8, 0.7, 0.8, 0.6]

    merchants = []
    for i in range(1, n + 1):
        city = rng.choices(cities, weights=city_weights, k=1)[0]
        category = rng.choice(categories)
        base_lat, base_lon = CITIES[city]
        merchants.append(
            {
                "merchant_id": f"M-{i:03d}",
                "name": f"{rng.choice(CATEGORIES[category])} {i:03d}",
                "city": city,
                "category": category,
                # dispersion d'environ +/- 5 km autour du centre-ville
                "latitude": round(base_lat + rng.uniform(-0.05, 0.05), 6),
                "longitude": round(base_lon + rng.uniform(-0.05, 0.05), 6),
            }
        )
    return merchants


def main():
    parser = argparse.ArgumentParser(description="Referentiel merchants SahlPay")
    parser.add_argument("--n", type=int, default=300, help="nombre de commercants")
    parser.add_argument("--out", default="data/merchants.json", help="fichier de sortie (JSON Lines)")
    parser.add_argument("--seed", type=int, default=42, help="graine aleatoire (reproductibilite)")
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    merchants = build_merchants(args.n, args.seed)
    with out.open("w", encoding="utf-8") as fh:
        for merchant in merchants:
            fh.write(json.dumps(merchant, ensure_ascii=False) + "\n")
    print(f"{len(merchants)} commercants ecrits dans {out}")


if __name__ == "__main__":
    main()
