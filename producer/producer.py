"""Generateur d'evenements de transactions SahlPay.ma.

Deux modes, comme demande dans le cahier des charges :

  * ``--mode file``  : ecrit un historique au format JSON Lines
                       (Parties 2 et 3, traitement batch)

        python producer/producer.py --mode file --n 50000 \
            --out data/transactions_history.jsonl

  * ``--mode kafka`` : publie en continu dans le topic Kafka ``transactions``
                       (Parties 4 et 5, temps reel)

        python producer/producer.py --mode kafka --rate 20

Un evenement = un changement d'etat d'une transaction. Le cycle de vie est
INITIATED -> AUTHORIZED -> COMPLETED, avec les branches REJECTED (refus) et
REVERSED (annulation apres encaissement).

Imperfections volontaires (demandees au 2.1) :
  * ~5 % d'evenements en retard (jusqu'a 15 minutes : telephone hors reseau) ;
  * ~2 % de doublons stricts (meme ``tx_id`` et meme ``status``) ;
  * scenarios de fraude injectes : appareil partage par de nombreux comptes,
    transferts circulaires A -> B -> C -> A, comptes collecteurs.

Format d'un evenement :

    {"tx_id": "TX-00004512", "type": "P2M", "payer_wallet": "W-10233",
     "payee_id": "M-087", "device_id": "D-5f2a91", "city": "Rabat",
     "channel": "QR", "status": "COMPLETED", "amount_mad": 320.00,
     "event_time": "2026-11-15T12:34:56Z"}
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------------
# Parametres du modele de donnees
# --------------------------------------------------------------------------
CHANNELS = ["APP", "QR", "USSD"]
CHANNEL_WEIGHTS = [0.45, 0.40, 0.15]

# Probabilites des imperfections
LATE_RATIO = 0.05           # ~5 % d'evenements en retard
LATE_MAX_SECONDS = 15 * 60  # jusqu'a 15 minutes
DUPLICATE_RATIO = 0.02      # ~2 % de doublons stricts

# Issues d'une transaction (apres INITIATED)
OUTCOME_WEIGHTS = {
    "COMPLETED": 0.84,   # INITIATED -> AUTHORIZED -> COMPLETED
    "REJECTED": 0.12,    # INITIATED -> REJECTED
    "REVERSED": 0.04,    # INITIATED -> AUTHORIZED -> COMPLETED -> REVERSED
}

# Volumetrie des entites
N_WALLETS = 2000
N_DEVICES = 1500

# Scenarios de fraude injectes
N_SHARED_DEVICES = 4         # un appareil utilise par de nombreux comptes
ACCOUNTS_PER_SHARED_DEVICE = 25
N_RINGS = 6                  # cycles de transferts A -> B -> C -> A
RING_SIZE = 3
N_COLLECTORS = 5             # comptes recevant de nombreux petits virements
FRAUD_EVENT_RATIO = 0.06     # part des transactions issues de ces scenarios

ISO = "%Y-%m-%dT%H:%M:%SZ"


def iso(dt: datetime) -> str:
    """Formate un instant en ISO-8601 UTC (suffixe Z), sans microsecondes."""
    return dt.astimezone(timezone.utc).strftime(ISO)


class Catalog:
    """Univers de reference : wallets, appareils, commercants, scenarios."""

    def __init__(self, merchants_path: Path, rng: random.Random):
        self.rng = rng
        self.merchants = self._load_merchants(merchants_path)
        self.cities = sorted({m["city"] for m in self.merchants})
        self.wallets = [f"W-{10000 + i}" for i in range(N_WALLETS)]
        self.devices = [f"D-{i:06x}" for i in range(N_DEVICES)]

        # Chaque wallet "normal" possede son propre appareil et sa ville.
        self.wallet_device = {w: rng.choice(self.devices) for w in self.wallets}
        self.wallet_city = {w: rng.choice(self.cities) for w in self.wallets}

        # --- Scenario 1 : appareils partages par de nombreux comptes ---
        self.shared_devices = [f"D-SHARED{i:02d}" for i in range(N_SHARED_DEVICES)]
        self.shared_device_wallets = {}
        for device in self.shared_devices:
            self.shared_device_wallets[device] = rng.sample(
                self.wallets, ACCOUNTS_PER_SHARED_DEVICE
            )

        # --- Scenario 2 : anneaux de transferts circulaires A -> B -> C -> A ---
        self.rings = [rng.sample(self.wallets, RING_SIZE) for _ in range(N_RINGS)]

        # --- Scenario 3 : comptes collecteurs (beaucoup de petits virements) ---
        self.collectors = rng.sample(self.wallets, N_COLLECTORS)

    @staticmethod
    def _load_merchants(path: Path):
        if not path.exists():
            sys.exit(
                f"Referentiel introuvable : {path}\n"
                "Generez-le d'abord : python producer/gen_merchants.py"
            )
        merchants = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    merchants.append(json.loads(line))
        return merchants


class TransactionGenerator:
    """Produit des evenements de transaction, un par changement d'etat."""

    def __init__(self, catalog: Catalog, rng: random.Random, start: datetime):
        self.catalog = catalog
        self.rng = rng
        self.clock = start
        self.counter = 0
        self.pending_duplicates = []

    # -- choix d'une transaction -------------------------------------------
    def _next_tx_id(self) -> str:
        self.counter += 1
        return f"TX-{self.counter:08d}"

    def _normal_transaction(self):
        """Transaction ordinaire : paiement chez un commercant ou transfert P2P."""
        rng = self.rng
        payer = rng.choice(self.catalog.wallets)
        device = self.catalog.wallet_device[payer]

        if rng.random() < 0.65:  # P2M : paiement chez un commercant
            merchant = rng.choice(self.catalog.merchants)
            return {
                "type": "P2M",
                "payer_wallet": payer,
                "payee_id": merchant["merchant_id"],
                "device_id": device,
                "city": merchant["city"],          # la ville est celle du point de vente
                "amount": round(rng.lognormvariate(4.2, 0.9), 2),
            }

        payee = rng.choice(self.catalog.wallets)   # P2P : transfert entre particuliers
        while payee == payer:
            payee = rng.choice(self.catalog.wallets)
        return {
            "type": "P2P",
            "payer_wallet": payer,
            "payee_id": payee,
            "device_id": device,
            "city": self.catalog.wallet_city[payer],
            "amount": round(rng.lognormvariate(5.0, 1.0), 2),
        }

    def _fraud_transaction(self):
        """Transaction issue d'un des trois scenarios suspects injectes."""
        rng = self.rng
        scenario = rng.choice(["shared_device", "ring", "collector"])

        if scenario == "shared_device":
            device = rng.choice(self.catalog.shared_devices)
            payer = rng.choice(self.catalog.shared_device_wallets[device])
            merchant = rng.choice(self.catalog.merchants)
            return {
                "type": "P2M",
                "payer_wallet": payer,
                "payee_id": merchant["merchant_id"],
                "device_id": device,
                "city": merchant["city"],
                "amount": round(rng.uniform(80, 900), 2),
            }

        if scenario == "ring":
            ring = rng.choice(self.catalog.rings)
            i = rng.randrange(len(ring))
            payer, payee = ring[i], ring[(i + 1) % len(ring)]
            return {
                "type": "P2P",
                "payer_wallet": payer,
                "payee_id": payee,
                "device_id": self.catalog.wallet_device[payer],
                "city": self.catalog.wallet_city[payer],
                "amount": round(rng.uniform(1500, 9000), 2),
            }

        collector = rng.choice(self.catalog.collectors)   # compte collecteur
        payer = rng.choice(self.catalog.wallets)
        while payer == collector:
            payer = rng.choice(self.catalog.wallets)
        return {
            "type": "P2P",
            "payer_wallet": payer,
            "payee_id": collector,
            "device_id": self.catalog.wallet_device[payer],
            "city": self.catalog.wallet_city[payer],
            "amount": round(rng.uniform(20, 180), 2),   # de nombreux petits virements
        }

    # -- cycle de vie -------------------------------------------------------
    def _statuses(self):
        outcome = self.rng.choices(
            list(OUTCOME_WEIGHTS), weights=list(OUTCOME_WEIGHTS.values()), k=1
        )[0]
        if outcome == "REJECTED":
            return ["INITIATED", "REJECTED"]
        if outcome == "REVERSED":
            return ["INITIATED", "AUTHORIZED", "COMPLETED", "REVERSED"]
        return ["INITIATED", "AUTHORIZED", "COMPLETED"]

    def next_events(self):
        """Renvoie les evenements du prochain changement d'etat a emettre.

        La liste contient les evenements d'une transaction, ou bien un doublon
        strict d'un evenement deja emis (meme tx_id, meme status).
        """
        rng = self.rng

        # Un doublon en attente est reemis tel quel, plus loin dans le flux.
        if self.pending_duplicates and rng.random() < 0.5:
            return [self.pending_duplicates.pop()]

        base = (
            self._fraud_transaction()
            if rng.random() < FRAUD_EVENT_RATIO
            else self._normal_transaction()
        )
        tx_id = self._next_tx_id()
        channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

        # L'horloge logique avance d'un pas aleatoire a chaque transaction.
        self.clock += timedelta(milliseconds=rng.randint(20, 400))
        events = []
        event_time = self.clock
        for status in self._statuses():
            # Quelques secondes separent deux etats de la meme transaction.
            event_time = event_time + timedelta(seconds=rng.randint(1, 20))

            # ~5 % d'evenements en retard : l'horodatage metier reste correct,
            # mais l'evenement est publie bien plus tard (telephone hors reseau).
            if rng.random() < LATE_RATIO:
                emitted = event_time + timedelta(
                    seconds=rng.randint(60, LATE_MAX_SECONDS)
                )
                self.clock = max(self.clock, emitted)

            event = {
                "tx_id": tx_id,
                "type": base["type"],
                "payer_wallet": base["payer_wallet"],
                "payee_id": base["payee_id"],
                "device_id": base["device_id"],
                "city": base["city"],
                "channel": channel,
                "status": status,
                "amount_mad": base["amount"],
                "event_time": iso(event_time),
            }
            events.append(event)

            # ~2 % de doublons stricts, reemis un peu plus tard dans le flux.
            if rng.random() < DUPLICATE_RATIO:
                self.pending_duplicates.append(dict(event))

        return events


# --------------------------------------------------------------------------
# Modes de sortie
# --------------------------------------------------------------------------
def run_file_mode(args, generator: TransactionGenerator) -> None:
    """Ecrit --n evenements dans un fichier JSON Lines."""
    out = Path(args.out)
    if out.parent != Path(""):
        out.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with out.open("w", encoding="utf-8") as fh:
        while written < args.n:
            for event in generator.next_events():
                if written >= args.n:
                    break
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
                written += 1
                if written % 10000 == 0:
                    print(f"  ... {written} evenements", file=sys.stderr)

    print(f"{written} evenements ecrits dans {out}")
    print(
        "Copie vers la couche Bronze :\n"
        f"  mc cp {out} local/lake/bronze/history/{out.name}"
    )


def run_kafka_mode(args, generator: TransactionGenerator) -> None:
    """Publie en continu dans le topic Kafka, a --rate evenements par seconde."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("kafka-python est requis : pip install kafka-python==2.0.2")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        # La cle de partitionnement est le tx_id : tous les etats d'une meme
        # transaction arrivent dans la meme partition, donc dans l'ordre.
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

    running = {"on": True}

    def stop(_signum, _frame):
        running["on"] = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    started = time.time()
    deadline = started + args.duration if args.duration else None
    print(
        f"Publication vers {args.bootstrap} / topic {args.topic} "
        f"a {args.rate} evenements/s (Ctrl+C pour arreter)"
    )

    while running["on"]:
        if deadline and time.time() >= deadline:
            break
        for event in generator.next_events():
            producer.send(args.topic, key=event["tx_id"], value=event)
            sent += 1
            if sent % 100 == 0:
                elapsed = time.time() - started
                print(f"  {sent} evenements envoyes ({sent / max(elapsed, 1e-9):.1f}/s)")
            time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"Arret : {sent} evenements publies dans {args.topic}.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generateur de transactions SahlPay (fichier ou Kafka)"
    )
    parser.add_argument("--mode", choices=["file", "kafka"], default="file")
    parser.add_argument("--n", type=int, default=50000,
                        help="mode file : nombre d'evenements a ecrire")
    parser.add_argument("--rate", type=float, default=20,
                        help="mode kafka : evenements par seconde")
    parser.add_argument("--duration", type=float, default=0,
                        help="mode kafka : duree en secondes (0 = sans limite)")
    parser.add_argument("--out", default="data/transactions_history.jsonl",
                        help="mode file : fichier de sortie")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="mode kafka : brokers (host:port[,host:port])")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--merchants", default="data/merchants.json",
                        help="referentiel des commercants (JSON Lines)")
    parser.add_argument("--seed", type=int, default=2026,
                        help="graine aleatoire (reproductibilite)")
    parser.add_argument("--start", default=None,
                        help="instant de depart ISO-8601, ex. 2026-11-15T08:00:00Z "
                             "(defaut : maintenant en mode kafka, "
                             "il y a 7 jours en mode file)")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rng = random.Random(args.seed)

    if args.start:
        start = datetime.strptime(args.start, ISO).replace(tzinfo=timezone.utc)
    elif args.mode == "file":
        # L'historique batch couvre les 7 derniers jours.
        start = datetime.now(timezone.utc) - timedelta(days=7)
    else:
        start = datetime.now(timezone.utc)

    catalog = Catalog(Path(args.merchants), rng)
    generator = TransactionGenerator(catalog, rng, start)

    if args.mode == "file":
        run_file_mode(args, generator)
    else:
        run_kafka_mode(args, generator)


if __name__ == "__main__":
    main()
