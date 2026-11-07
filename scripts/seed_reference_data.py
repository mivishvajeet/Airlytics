"""Script to generate and seed airport reference data from OurAirports."""

import csv
import io
import os
import sys
from typing import List, Dict, Any

REFERENCE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reference")
OUTPUT_CSV_PATH = os.path.join(REFERENCE_DIR, "airports.csv")

# Baseline curated list of top global and European commercial hub airports
# Columns match OurAirports official open schema
SEED_AIRPORTS = [
    {
        "id": "2434", "ident": "EHAM", "type": "large_airport", "name": "Amsterdam Airport Schiphol",
        "latitude_deg": "52.308601", "longitude_deg": "4.76389", "elevation_ft": "-11",
        "continent": "EU", "iso_country": "NL", "iso_region": "NL-NH", "municipality": "Amsterdam",
        "scheduled_service": "yes", "gps_code": "EHAM", "iata_code": "AMS", "local_code": ""
    },
    {
        "id": "2433", "ident": "EGLL", "type": "large_airport", "name": "London Heathrow Airport",
        "latitude_deg": "51.4706", "longitude_deg": "-0.461941", "elevation_ft": "83",
        "continent": "EU", "iso_country": "GB", "iso_region": "GB-ENG", "municipality": "London",
        "scheduled_service": "yes", "gps_code": "EGLL", "iata_code": "LHR", "local_code": ""
    },
    {
        "id": "2513", "ident": "EDDF", "type": "large_airport", "name": "Frankfurt am Main Airport",
        "latitude_deg": "50.033333", "longitude_deg": "8.570556", "elevation_ft": "364",
        "continent": "EU", "iso_country": "DE", "iso_region": "DE-HE", "municipality": "Frankfurt am Main",
        "scheduled_service": "yes", "gps_code": "EDDF", "iata_code": "FRA", "local_code": ""
    },
    {
        "id": "2520", "ident": "LFPG", "type": "large_airport", "name": "Charles de Gaulle International Airport",
        "latitude_deg": "49.012798", "longitude_deg": "2.55", "elevation_ft": "392",
        "continent": "EU", "iso_country": "FR", "iso_region": "FR-IDF", "municipality": "Paris",
        "scheduled_service": "yes", "gps_code": "LFPG", "iata_code": "CDG", "local_code": ""
    },
    {
        "id": "2521", "ident": "LFPO", "type": "large_airport", "name": "Paris-Orly Airport",
        "latitude_deg": "48.723333", "longitude_deg": "2.379444", "elevation_ft": "291",
        "continent": "EU", "iso_country": "FR", "iso_region": "FR-IDF", "municipality": "Paris",
        "scheduled_service": "yes", "gps_code": "LFPO", "iata_code": "ORY", "local_code": ""
    },
    {
        "id": "2514", "ident": "EDDM", "type": "large_airport", "name": "Munich Airport",
        "latitude_deg": "48.353799", "longitude_deg": "11.786", "elevation_ft": "1487",
        "continent": "EU", "iso_country": "DE", "iso_region": "DE-BY", "municipality": "Munich",
        "scheduled_service": "yes", "gps_code": "EDDM", "iata_code": "MUC", "local_code": ""
    },
    {
        "id": "2581", "ident": "LEMD", "type": "large_airport", "name": "Adolfo Suárez Madrid–Barajas Airport",
        "latitude_deg": "40.471926", "longitude_deg": "-3.56264", "elevation_ft": "1998",
        "continent": "EU", "iso_country": "ES", "iso_region": "ES-M", "municipality": "Madrid",
        "scheduled_service": "yes", "gps_code": "LEMD", "iata_code": "MAD", "local_code": ""
    },
    {
        "id": "2582", "ident": "LEBL", "type": "large_airport", "name": "Josep Tarradellas Barcelona-El Prat Airport",
        "latitude_deg": "41.2971", "longitude_deg": "2.07846", "elevation_ft": "12",
        "continent": "EU", "iso_country": "ES", "iso_region": "ES-CT", "municipality": "Barcelona",
        "scheduled_service": "yes", "gps_code": "LEBL", "iata_code": "BCN", "local_code": ""
    },
    {
        "id": "2569", "ident": "LSZH", "type": "large_airport", "name": "Zurich Airport",
        "latitude_deg": "47.464699", "longitude_deg": "8.54917", "elevation_ft": "1416",
        "continent": "EU", "iso_country": "CH", "iso_region": "CH-ZH", "municipality": "Zurich",
        "scheduled_service": "yes", "gps_code": "LSZH", "iata_code": "ZRH", "local_code": ""
    },
    {
        "id": "2544", "ident": "EBBR", "type": "large_airport", "name": "Brussels Airport",
        "latitude_deg": "50.901402", "longitude_deg": "4.48444", "elevation_ft": "184",
        "continent": "EU", "iso_country": "BE", "iso_region": "BE-BRU", "municipality": "Brussels",
        "scheduled_service": "yes", "gps_code": "EBBR", "iata_code": "BRU", "local_code": ""
    },
    {
        "id": "3628", "ident": "KJFK", "type": "large_airport", "name": "John F. Kennedy International Airport",
        "latitude_deg": "40.639801", "longitude_deg": "-73.7789", "elevation_ft": "13",
        "continent": "NA", "iso_country": "US", "iso_region": "US-NY", "municipality": "New York",
        "scheduled_service": "yes", "gps_code": "KJFK", "iata_code": "JFK", "local_code": "JFK"
    },
    {
        "id": "3752", "ident": "KORD", "type": "large_airport", "name": "Chicago O'Hare International Airport",
        "latitude_deg": "41.9786", "longitude_deg": "-87.9048", "elevation_ft": "668",
        "continent": "NA", "iso_country": "US", "iso_region": "US-IL", "municipality": "Chicago",
        "scheduled_service": "yes", "gps_code": "KORD", "iata_code": "ORD", "local_code": "ORD"
    },
    {
        "id": "3632", "ident": "KLAX", "type": "large_airport", "name": "Los Angeles International Airport",
        "latitude_deg": "33.942501", "longitude_deg": "-118.407997", "elevation_ft": "125",
        "continent": "NA", "iso_country": "US", "iso_region": "US-CA", "municipality": "Los Angeles",
        "scheduled_service": "yes", "gps_code": "KLAX", "iata_code": "LAX", "local_code": "LAX"
    },
    {
        "id": "2195", "ident": "OMDB", "type": "large_airport", "name": "Dubai International Airport",
        "latitude_deg": "25.252799", "longitude_deg": "55.364398", "elevation_ft": "62",
        "continent": "AS", "iso_country": "AE", "iso_region": "AE-DU", "municipality": "Dubai",
        "scheduled_service": "yes", "gps_code": "OMDB", "iata_code": "DXB", "local_code": ""
    },
    {
        "id": "2188", "ident": "WSSS", "type": "large_airport", "name": "Singapore Changi Airport",
        "latitude_deg": "1.35019", "longitude_deg": "103.994003", "elevation_ft": "22",
        "continent": "AS", "iso_country": "SG", "iso_region": "SG-01", "municipality": "Singapore",
        "scheduled_service": "yes", "gps_code": "WSSS", "iata_code": "SIN", "local_code": ""
    }
]

FIELDNAMES = [
    "id", "ident", "type", "name", "latitude_deg", "longitude_deg",
    "elevation_ft", "continent", "iso_country", "iso_region", "municipality",
    "scheduled_service", "gps_code", "iata_code", "local_code"
]


def generate_seed_csv() -> str:
    """Generate the seed CSV file with standard headers and baseline airports."""
    os.makedirs(REFERENCE_DIR, exist_ok=True)
    with open(OUTPUT_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in SEED_AIRPORTS:
            writer.writerow(row)
    print(f"Generated airport reference dataset at: {OUTPUT_CSV_PATH} ({len(SEED_AIRPORTS)} hub airports)")
    return OUTPUT_CSV_PATH


if __name__ == "__main__":
    generate_seed_csv()
