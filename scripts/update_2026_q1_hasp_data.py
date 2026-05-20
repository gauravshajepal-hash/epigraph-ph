from __future__ import annotations

import json
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
NORMALIZED = DIST / "data" / "normalized"
SOURCE_PDF = Path("/home/gaurav/Desktop/2026_Q1 HIV & AIDS Surveillance of the Philippines.pdf")
SOURCE_BASENAME = "2026_Q1_HIV_AIDS_Surveillance_of_the_Philippines.pdf"
SOURCE_DIST = DIST / "data" / "sources" / SOURCE_BASENAME
SOURCE_URL = f"https://gauravshajepal-hash.github.io/epigraph-ph/data/sources/{SOURCE_BASENAME}"
PERIOD = "2026 Q1"
PERIOD_SORT = 202603
YEAR = 2026
QUARTER = "Q1"
FILENAME = SOURCE_BASENAME


REGION_LABELS = {
    "1": "Region 1",
    "2": "Region 2",
    "3": "Region 3",
    "4A": "Region 4A",
    "4B": "Region 4B",
    "5": "Region 5",
    "6": "Region 6",
    "7": "Region 7",
    "8": "Region 8",
    "9": "Region 9",
    "10": "Region 10",
    "11": "Region 11",
    "12": "Region 12",
    "BARMM": "BARMM",
    "CAR": "CAR",
    "CARAGA": "CARAGA",
    "NCR": "NCR",
    "NIR": "NIR",
}


NATIONAL = {
    "estimated_plhiv": 288000,
    "diagnosed_plhiv": 157350,
    "diagnosed_pct": 55,
    "on_art": 108367,
    "on_art_pct": 69,
    "vl_tested": 61413,
    "vl_tested_pct_of_on_art": 57,
    "vl_tested_pct_of_eligible": 59,
    "vl_suppressed": 59540,
    "vl_suppressed_pct_of_tested": 97,
    "suppressed_pct_of_on_art": 55,
    "vl_unsuppressed": 1873,
    "eligible_for_vl": 103825,
    "target_diagnosed": 273600,
    "target_on_art": 259920,
    "target_suppressed": 246924,
    "gap_undiagnosed": 116250,
    "gap_to_art": 86553,
    "gap_return_to_art": 40928,
    "gap_to_vl_test": 185511,
    "gap_to_suppression": 187384,
}


PREP_QUARTERLY = [
    ("2023 Q1", 3893),
    ("2023 Q2", 4375),
    ("2023 Q3", 4358),
    ("2023 Q4", 4090),
    ("2024 Q1", 5265),
    ("2024 Q2", 6972),
    ("2024 Q3", 6606),
    ("2024 Q4", 6847),
    ("2025 Q1", 8600),
    ("2025 Q2", 8816),
    ("2025 Q3", 7662),
    ("2025 Q4", 7097),
    (PERIOD, 8229),
]


DIAGNOSIS = {
    "new_cases": 4633,
    "new_cases_male": 4381,
    "new_cases_female": 252,
    "new_cases_male_pct": 95,
    "new_cases_female_pct": 5,
    "new_cases_age_min": 2,
    "new_cases_age_max": 77,
    "new_cases_age_median": 28,
    "new_cases_age_missing": 88,
    "new_cases_yoy_change_pct": 9,
    "advanced_hiv_disease": 1104,
    "advanced_hiv_disease_pct": 24,
    "non_advanced_hiv": 3529,
    "non_advanced_missing_criteria": 1196,
    "non_advanced_missing_criteria_pct": 33,
    "average_cases_per_day": 51,
    "average_cases_per_day_change_pct": -11,
    "cumulative_cases": 168079,
    "cumulative_male": 158670,
    "cumulative_male_pct": 94,
    "cumulative_female": 9399,
    "cumulative_female_pct": 6,
    "sex_missing": 10,
    "cumulative_age_missing": 211,
    "crcls_reporting": 195,
    "crcl_confirmed_pct": 58,
}


NEW_CASES_AGE = {
    "age_under_15": (23, 1),
    "age_15_24": (1443, 31),
    "age_25_34": (2118, 46),
    "age_35_49": (845, 18),
    "age_50_plus": (116, 3),
}


CUMULATIVE_AGE = {
    "age_under_15": (588, 1),
    "age_15_24": (50457, 30),
    "age_25_34": (82894, 49),
    "age_35_49": (29650, 18),
    "age_50_plus": (4279, 3),
}


AGE_INCREASE = {
    "age_under_15": (278, 588, 111.51),
    "age_15_24": (27190, 50457, 85.57),
    "age_25_34": (47701, 82894, 73.78),
    "age_35_49": (16332, 29650, 81.55),
    "age_50_plus": (2459, 4279, 74.01),
    "total": (93960, 167868, 78.66),
}


MONTHLY_CASES = {
    2023: [1443, 1289, 2076, 1236, 1249, 1516, 1553, 1570, 1567, 1499, 1277, 950],
    2024: [1081, 1159, 1161, 1218, 1334, 2761, 1652, 1570, 1362, 1549, 1305, 1355],
    2025: [1807, 1689, 1583, 1606, 1594, 1715, 2176, 1607, 1708, 1537, 1330, 1407],
    2026: [1690, 1407, 1536],
}


MONTHLY_AVERAGES = {2023: 1435, 2024: 1459, 2025: 1647, 2026: 1544}


NEW_CASES_REGION = [
    ("NCR", 989, 21),
    ("4A", 808, 17),
    ("3", 551, 12),
    ("12", 277, 6),
    ("11", 263, 6),
    ("7", 228, 5),
    ("6", 216, 5),
    ("1", 191, 4),
    ("10", 183, 4),
    ("5", 158, 3),
    ("9", 137, 3),
    ("NIR", 133, 3),
    ("2", 108, 2),
    ("4B", 101, 2),
    ("8", 101, 2),
    ("CARAGA", 84, 2),
    ("BARMM", 42, 1),
    ("CAR", 37, 1),
]


REGION_CUMULATIVE = [
    ("NCR", 23951, 25, 52054, 31, 39),
    ("4A", 16982, 18, 28211, 17, 19),
    ("3", 10721, 11, 17760, 11, 18),
    ("7", 5960, 6, 12137, 7, 8),
    ("11", 5329, 6, 9464, 6, 5),
    ("6", 4634, 5, 7162, 4, 9),
    ("12", 3210, 3, 5046, 3, 10),
    ("1", 3219, 3, 4892, 3, 10),
    ("10", 3055, 3, 4667, 3, 10),
    ("NIR", 3029, 3, 4548, 3, 7),
    ("5", 2647, 3, 3977, 2, 9),
    ("9", 2117, 2, 3107, 2, 13),
    ("2", 2120, 2, 3077, 2, 8),
    ("4B", 2176, 2, 3059, 2, 5),
    ("8", 2036, 2, 3052, 2, 6),
    ("CARAGA", 1598, 2, 2373, 1, 9),
    ("CAR", 868, 1, 1465, 1, 7),
    ("BARMM", 564, 1, 766, 1, 3),
]


MOT = {
    "q1": {
        "total_with_mot": 4619,
        "male": 4367,
        "female": 252,
        "sexual_contact": (4214, 91),
        "sexual_contact_male": 3993,
        "sexual_contact_female": 221,
        "male_male": 3095,
        "male_male_female": 567,
        "male_female": 552,
        "male_female_male": 331,
        "male_female_female": 221,
        "needle": 2,
        "needle_male": 1,
        "needle_female": 1,
        "mother_to_child": 13,
        "mother_to_child_male": 9,
        "mother_to_child_female": 4,
        "blood_products": 0,
        "needlestick": 0,
    },
    "since_2020": {
        "total_with_mot": 94272,
        "male": 89385,
        "female": 4887,
        "sexual_contact": 90190,
        "sexual_contact_male": 85662,
        "sexual_contact_female": 4528,
        "male_male": 63349,
        "male_male_female": 15964,
        "male_female": 10877,
        "male_female_male": 6349,
        "male_female_female": 4528,
        "needle": 459,
        "needle_male": 430,
        "needle_female": 29,
        "mother_to_child": 245,
        "mother_to_child_male": 128,
        "mother_to_child_female": 117,
        "blood_products": 0,
        "needlestick": 0,
    },
    "cumulative": {
        "total_with_mot": 167966,
        "male": 158562,
        "female": 9394,
        "missing_mot": 4133,
        "sexual_contact": 160712,
        "sexual_contact_pct": 96,
        "sexual_contact_male": 152003,
        "sexual_contact_female": 8709,
        "male_male": 102136,
        "male_male_female": 36171,
        "male_female": 22405,
        "male_female_male": 13696,
        "male_female_female": 8709,
        "needle": 2647,
        "needle_pct": 2,
        "needle_male": 2490,
        "needle_female": 157,
        "mother_to_child": 444,
        "mother_to_child_male": 233,
        "mother_to_child_female": 211,
        "blood_products": 19,
        "blood_products_male": 5,
        "blood_products_female": 14,
        "needlestick": 3,
        "needlestick_male": 2,
        "needlestick_female": 1,
    },
}


AHD = {
    "cumulative_count": 50805,
    "cumulative_pct": 31,
    "missing_criteria_count": 44264,
    "missing_criteria_pct": 28,
    "annual_pct": {
        2011: 9,
        2012: 13,
        2013: 17,
        2014: 23,
        2015: 28,
        2016: 32,
        2017: 32,
        2018: 36,
        2019: 35,
        2020: 37,
        2021: 31,
        2022: 36,
        2023: 35,
        2024: 29,
        2025: 24,
        2026: 24,
    },
}


ART = {
    "newly_enrolled": 4716,
    "first_line": 4665,
    "first_line_pct": 99,
    "second_line": 1,
    "second_line_pct": 1,
    "other_line": 50,
    "other_line_pct": 1,
    "age_under_15": 22,
    "age_under_15_pct": 1,
    "age_15_24": 1534,
    "age_15_24_pct": 33,
    "age_25_34": 2175,
    "age_25_34_pct": 46,
    "age_35_49": 887,
    "age_35_49_pct": 19,
    "age_50_plus": 98,
    "age_50_plus_pct": 2,
    "median_baseline_cd4": 182,
    "missing_baseline_cd4": 2806,
    "on_art": 108367,
    "current_age_min": 1,
    "current_age_max": 84,
    "current_age_median": 33,
    "on_art_male": 104207,
    "on_art_female": 4159,
    "on_art_sex_missing": 1,
    "ever_enrolled_total": 146164,
    "currently_receiving_pct_of_ever_enrolled": 74,
    "on_art_first_line": 106292,
    "on_art_first_line_pct": 98,
    "on_art_second_line": 965,
    "on_art_second_line_pct": 1,
    "on_art_other_line": 1110,
    "on_art_other_line_pct": 1,
    "no_longer_receiving": 30666,
    "no_longer_receiving_pct": 21,
    "lost_to_follow_up": 30644,
    "stopped": 4,
    "transferred_overseas": 18,
    "dead": 7131,
}


TREATMENT_OUTCOME = [
    ("NCR", 43052, 14425, 1752, 0, 0, 59229, 24),
    ("7", 7646, 3007, 709, 0, 0, 11362, 26),
    ("4A", 13163, 2642, 681, 1, 0, 16487, 16),
    ("3", 9895, 2216, 1100, 5, 1, 13217, 17),
    ("11", 6774, 2034, 336, 0, 0, 9144, 22),
    ("12", 3680, 844, 159, 0, 0, 4683, 18),
    ("10", 2755, 780, 201, 0, 0, 3736, 21),
    ("NIR", 2644, 647, 392, 0, 0, 3683, 18),
    ("6", 5049, 634, 594, 7, 0, 6284, 10),
    ("4B", 1338, 603, 115, 0, 0, 2056, 29),
    ("8", 1371, 568, 122, 0, 0, 2061, 28),
    ("5", 1986, 564, 224, 0, 0, 2774, 20),
    ("1", 2340, 423, 173, 0, 0, 2936, 14),
    ("9", 1710, 407, 166, 4, 0, 2287, 18),
    ("CAR", 1296, 263, 83, 0, 0, 1642, 16),
    ("CARAGA", 1392, 241, 122, 0, 3, 1758, 14),
    ("2", 1880, 223, 147, 1, 0, 2251, 10),
    ("BARMM", 395, 119, 55, 0, 0, 569, 21),
]


VL_FACILITY = [
    ("NCR", 43054, 22979, 53, 22455, 98),
    ("4A", 13163, 8168, 62, 7945, 97),
    ("3", 9895, 6145, 62, 5940, 97),
    ("7", 7646, 3831, 50, 3736, 98),
    ("11", 6774, 4005, 59, 3871, 97),
    ("6", 5049, 3548, 70, 3463, 98),
    ("12", 3680, 1405, 38, 1322, 94),
    ("10", 2755, 1693, 61, 1603, 95),
    ("NIR", 2644, 1801, 68, 1763, 98),
    ("1", 2340, 1228, 52, 1174, 96),
    ("5", 1986, 1384, 70, 1280, 92),
    ("2", 1880, 1434, 76, 1386, 97),
    ("9", 1710, 932, 55, 894, 96),
    ("CARAGA", 1392, 765, 55, 687, 90),
    ("8", 1371, 559, 41, 530, 95),
    ("4B", 1338, 380, 28, 374, 98),
    ("CAR", 1296, 866, 67, 848, 98),
    ("BARMM", 395, 289, 73, 268, 93),
]


VL_ANNUAL = {
    "years": [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026],
    "plhiv_on_art": [39162, 43367, 51863, 63221, 75300, 90568, 100671, 108367],
    "vl_tested": [11269, 7675, 17129, 17957, 31330, 41746, 58887, 61413],
    "vl_suppressed": [9240, 6457, 14547, 15851, 27468, 36633, 57182, 59540],
    "pct_tested": [29, 18, 33, 25, 42, 46, 59, 57],
    "pct_suppressed": [82, 84, 85, 88, 88, 88, 97, 97],
}


MORTALITY = {
    "new_deaths": 477,
    "new_deaths_yoy_change_pct": 2,
    "cumulative_deaths": 10727,
    "deaths_2020_2026": 5387,
    "cumulative_deaths_with_ahd": 5105,
    "cumulative_deaths_with_ahd_pct": 48,
    "death_age_0_14": 72,
    "death_age_0_14_pct": 1,
    "death_age_15_24": 1486,
    "death_age_15_24_pct": 14,
    "death_age_25_34": 4783,
    "death_age_25_34_pct": 45,
    "death_age_35_49": 2811,
    "death_age_35_49_pct": 26,
    "death_age_50_plus": 574,
    "death_age_50_plus_pct": 5,
    "death_age_missing_pct": 9,
    "q1_death_age_known_total": 204,
    "q1_death_age_missing_inferred": 273,
    "q1_death_age": {
        "age_0_14": (2, 1),
        "age_15_24": (24, 12),
        "age_25_34": (98, 48),
        "age_35_49": (61, 30),
        "age_50_plus": (19, 10),
    },
}


PREGNANT = {
    "q1_reported": 42,
    "q1_age_min": 15,
    "q1_age_max": 36,
    "q1_age_median": 24,
    "q1_yoy_change_pct": 8,
    "cumulative_reported": 1220,
    "past_year_diagnosed": 236,
    "past_year_age_15_24": 125,
    "past_year_age_15_24_pct": 53,
    "past_year_age_25_34": 85,
    "past_year_age_25_34_pct": 36,
    "past_year_age_35_49": 26,
    "past_year_age_35_49_pct": 11,
    "alive": 236,
    "initiated_art": 181,
    "initiated_art_pct": 77,
    "on_art": 132,
    "on_art_pct_of_diagnosed": 56,
    "retained_art_pct_of_initiated": 73,
    "vl_tested": 47,
    "vl_tested_pct": 36,
    "vl_suppressed": 38,
    "vl_suppressed_pct": 81,
}


TGW = {
    "q1_reported": 29,
    "q1_age_15_24": 9,
    "q1_age_15_24_pct": 31,
    "q1_age_25_34": 12,
    "q1_age_25_34_pct": 41,
    "q1_age_35_49": 7,
    "q1_age_35_49_pct": 24,
    "q1_age_50_plus": 1,
    "q1_age_50_plus_pct": 3,
    "q1_age_min": 20,
    "q1_age_max": 67,
    "q1_age_median": 29,
    "cumulative_diagnosed": 3026,
    "cumulative_sexual_contact": 2984,
    "cumulative_sexual_contact_pct": 99,
    "cumulative_needle": 6,
    "cumulative_mtct": 1,
    "cumulative_mot_missing": 34,
    "cumulative_age_15_24": 865,
    "cumulative_age_15_24_pct": 29,
    "cumulative_age_25_34": 1487,
    "cumulative_age_25_34_pct": 49,
    "cumulative_age_35_49": 598,
    "cumulative_age_35_49_pct": 20,
    "cumulative_age_50_plus": 75,
    "cumulative_age_50_plus_pct": 2,
    "cumulative_age_missing": 1,
    "cumulative_age_min": 15,
    "cumulative_age_max": 75,
    "cumulative_age_median": 28,
    "alive": 2789,
    "alive_pct": 92,
    "initiated_art": 2608,
    "initiated_art_pct": 94,
    "on_art": 1814,
    "on_art_pct": 65,
    "vl_tested": 1073,
    "vl_tested_pct": 59,
    "vl_suppressed": 914,
    "vl_suppressed_pct": 85,
}


MIGRANT = {
    "q1_reported": 223,
    "q1_age_min": 19,
    "q1_age_max": 64,
    "q1_age_median": 34,
    "q1_male": 205,
    "q1_male_pct": 92,
    "q1_female": 18,
    "q1_female_pct": 8,
    "q1_sexual_contact": 211,
    "q1_sexual_contact_pct": 95,
    "q1_male_male": 144,
    "q1_male_male_pct": 64,
    "q1_male_male_female": 32,
    "q1_male_male_female_pct": 14,
    "q1_male_female": 35,
    "q1_male_female_pct": 16,
    "q1_mot_missing": 12,
    "q1_mot_missing_pct": 5,
    "q1_yoy_change_pct": 17,
    "five_year_change_pct": 11,
    "cumulative_reported": 11579,
    "cumulative_reported_pct": 7,
    "cumulative_sexual_contact": 11332,
    "cumulative_sexual_contact_pct": 98,
    "cumulative_needle": 20,
    "cumulative_blood": 9,
    "cumulative_needlestick": 4,
    "cumulative_mot_missing": 210,
    "cumulative_mot_missing_pct": 2,
    "alive": 10892,
    "alive_pct": 94,
    "initiated_art": 9097,
    "initiated_art_pct": 84,
    "on_art": 6823,
    "on_art_pct": 63,
    "vl_tested": 3964,
    "vl_tested_pct": 58,
    "vl_suppressed": 3586,
    "vl_suppressed_pct": 90,
}


TRANSACTIONAL = {
    "q1_reported": 500,
    "q1_reported_pct": 11,
    "q1_male": 488,
    "q1_male_pct": 98,
    "q1_female": 12,
    "q1_female_pct": 2,
    "q1_age_min": 15,
    "q1_age_max": 77,
    "q1_age_median": 34,
    "since_2020": 11154,
    "since_2020_pct_of_total": 59,
    "cumulative_reported": 18774,
    "cumulative_male": 18252,
    "cumulative_male_pct": 97,
    "cumulative_female": 522,
    "cumulative_female_pct": 3,
    "cumulative_accepted": 6383,
    "cumulative_accepted_pct": 34,
    "cumulative_paid": 9330,
    "cumulative_paid_pct": 50,
    "cumulative_both": 3061,
    "cumulative_both_pct": 16,
    "alive": 17401,
    "alive_pct": 93,
    "initiated_art": 15968,
    "initiated_art_pct": 92,
    "on_art": 11631,
    "on_art_pct": 67,
    "vl_tested": 6745,
    "vl_tested_pct": 58,
    "vl_suppressed": 6522,
    "vl_suppressed_pct": 97,
}


TRANSACTIONAL_TABLE = [
    ("accepted", 177, 173, 4, "15-52 (25)", 3952, 3807, 145, "14-63 (26)", 6383, 6058, 325, "12-68 (26)"),
    ("paid_for_sex_only", 225, 223, 2, "17-77 (34)", 5266, 5236, 30, "15-80 (33)", 9330, 9275, 55, "13-80 (32)"),
    ("engaged_in_both", 98, 92, 6, "16-71 (31)", 1936, 1881, 55, "14-73 (29)", 3061, 2919, 142, "14-73 (29)"),
]


REGIONAL_CASCADE = [
    ("1", 9500, 4619, 49, 3217, 70, 1795, 56, 1735, 97, 54),
    ("2", 5800, 2853, 49, 2231, 78, 1571, 70, 1528, 97, 68),
    ("3", 34300, 16253, 47, 11445, 70, 6982, 61, 6766, 97, 59),
    ("4A", 50000, 26620, 53, 18517, 70, 10806, 58, 10525, 97, 57),
    ("4B", 4900, 2840, 58, 1810, 64, 673, 37, 662, 98, 37),
    ("5", 7200, 3634, 50, 2581, 71, 1646, 64, 1554, 94, 60),
    ("6", 13400, 6358, 47, 5115, 80, 3523, 69, 3437, 98, 67),
    ("7", 21400, 11318, 53, 6563, 58, 3497, 53, 3401, 97, 52),
    ("8", 5400, 2852, 53, 1823, 64, 859, 47, 827, 96, 45),
    ("9", 4900, 2889, 59, 1995, 69, 1065, 53, 1030, 97, 52),
    ("10", 8000, 4356, 54, 3043, 70, 1821, 60, 1733, 95, 57),
    ("11", 16700, 9026, 54, 6035, 67, 3584, 59, 3459, 97, 57),
    ("12", 9000, 4797, 53, 3612, 75, 1580, 44, 1524, 96, 42),
    ("BARMM", 1200, 716, 60, 447, 62, 266, 60, 254, 95, 57),
    ("CAR", 2700, 1383, 51, 1011, 73, 695, 69, 681, 98, 67),
    ("CARAGA", 3900, 2197, 56, 1645, 75, 937, 57, 859, 92, 52),
    ("NCR", 81300, 49567, 61, 31309, 63, 17170, 55, 16757, 98, 54),
    ("NIR", 8300, 3971, 48, 2948, 74, 1927, 65, 1889, 98, 64),
]


AGE_CASCADE = [
    ("children_under_15", 1700, 392, 23, 269, 69, 165, 61, 125, 78, 37),
    ("youth_15_24", 63800, 14361, 23, 10580, 74, 5322, 50, 5050, 95, 48),
    ("adults_25_plus", 222500, 142391, 64, 94567, 66, 55002, 58, 53503, 97, 57),
]


KEYPOP_CASCADE = [
    ("msm", 220000, 130081, 59, 92030, 71, 53213, 58, 51742, 97, 56),
    ("pwid", 2900, 2169, 75, 524, 24, 275, 52, 265, 96, 51),
    ("other_males", 42900, 12297, 29, 6716, 55, 3673, 55, 3600, 98, 54),
    ("other_females", 20600, 8139, 40, 3880, 48, 2263, 58, 2074, 92, 53),
]


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sort_value(label: str) -> int:
    text = str(label or "").strip()
    match = re.match(r"^(\d{4}) Q([1-4])$", text)
    if match:
        return int(match.group(1)) * 100 + int(match.group(2)) * 3
    match = re.match(r"^(\d{4})-(\d{4}) Q([1-4])$", text)
    if match:
        return int(match.group(2)) * 100 + int(match.group(3)) * 3
    match = re.match(r"^(\d{4})-(\d{2})$", text)
    if match:
        return int(match.group(1)) * 100 + int(match.group(2))
    match = re.match(r"^(\d{4})$", text)
    if match:
        return int(match.group(1)) * 100 + 12
    return 0


def point(period: str, value: float, region: str = "Philippines", subgroup: str = "") -> dict:
    return {"period": period, "region": region, "subgroup": subgroup, "value": float(value)}


def annual_point(year: int, label: str, value: float) -> dict:
    return {
        "year": year,
        "label": label,
        "sort_value": sort_value(label),
        "value": float(value),
        "filename": FILENAME,
        "source_url": SOURCE_URL,
    }


def upsert_period_point(points: list[dict], new_point: dict) -> list[dict]:
    label = str(new_point.get("period") or "")
    filtered = [row for row in points if str(row.get("period") or "") != label]
    filtered.append(new_point)
    filtered.sort(key=lambda row: sort_value(str(row.get("period") or "")))
    return filtered


def upsert_annual(points: list[dict], new_point: dict) -> list[dict]:
    year = int(new_point.get("year") or 0)
    filtered = [row for row in points if int(row.get("year") or 0) != year]
    filtered.append(new_point)
    filtered.sort(key=lambda row: int(row.get("sort_value") or sort_value(str(row.get("label") or row.get("year") or ""))))
    return filtered


def median(values: list[float]) -> float:
    values = sorted(float(value) for value in values)
    if not values:
        return 0.0
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def region_label(code: str) -> str:
    return REGION_LABELS.get(code, code)


def regional_rows_for_publication() -> list[dict]:
    rows = []
    for code, _, _, dx, _, tx, _, _, _, _, supp in REGIONAL_CASCADE:
        rows.append(
            {
                "region": region_label(code),
                "year": YEAR,
                "diagnosis": float(dx),
                "treatment": float(tx),
                "suppression": float(supp),
                "diagnosis_period": PERIOD,
                "treatment_period": PERIOD,
                "suppression_period": PERIOD,
                "diagnosis_source_url": SOURCE_URL,
                "treatment_source_url": SOURCE_URL,
                "suppression_source_url": SOURCE_URL,
                "diagnosis_filename": FILENAME,
                "treatment_filename": FILENAME,
                "suppression_filename": FILENAME,
                "mean_gap": round(((95 - dx) + (95 - tx) + (95 - supp)) / 3.0, 1),
            }
        )
    rows.sort(key=lambda row: row["mean_gap"])
    return rows


def regional_rows_for_dashboard() -> list[dict]:
    rows = []
    for code, _, _, dx, _, tx, _, _, _, _, supp in REGIONAL_CASCADE:
        rows.append({"region": code, "diagnosis": float(dx), "treatment": float(tx), "suppression": float(supp)})
    return rows


def fit_line(points: list[dict], x_key: str, y_key: str) -> tuple[float, float]:
    xs = [float(row[x_key]) for row in points]
    ys = [float(row[y_key]) for row in points]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if not var_x:
        return 0.0, mean_y
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / var_x
    intercept = mean_y - slope * mean_x
    return slope, intercept


def ranks(values: list[float]) -> list[float]:
    ordered = sorted((value, idx) for idx, value in enumerate(values))
    result = [0.0] * len(values)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        rank = (i + j + 2) / 2.0
        for _, idx in ordered[i : j + 1]:
            result[idx] = rank
        i = j + 1
    return result


def spearman(points: list[dict], x_key: str, y_key: str) -> float:
    xs = ranks([float(row[x_key]) for row in points])
    ys = ranks([float(row[y_key]) for row in points])
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    return 0.0 if not den_x or not den_y else num / (den_x * den_y)


def relationship_chart(chart_id: str, label: str, x_key: str, y_key: str, x_label: str, y_label: str) -> dict:
    rows = sorted(regional_rows_for_dashboard(), key=lambda row: row[x_key])
    slope, intercept = fit_line(rows, x_key, y_key)
    med_x = median([row[x_key] for row in rows])
    med_y = median([row[y_key] for row in rows])
    points = []
    residuals = []
    for row in rows:
        quadrant = ("high" if row[x_key] >= med_x else "low") + "-" + ("high" if row[y_key] >= med_y else "low")
        points.append({"region": row["region"], "x": row[x_key], "y": row[y_key], "quadrant": quadrant})
        residuals.append({"region": row["region"], "residual": round(row[y_key] - (intercept + slope * row[x_key]), 2)})
    residuals.sort(key=lambda row: row["residual"])
    min_x = min(row[x_key] for row in rows)
    max_x = max(row[x_key] for row in rows)
    return {
        "chart_id": chart_id,
        "label": label,
        "period_label": PERIOD,
        "x_metric": "diagnosis_coverage" if x_key == "diagnosis" else "art_coverage",
        "y_metric": "art_coverage" if y_key == "treatment" else "suppression_among_on_art",
        "x_label": x_label,
        "y_label": y_label,
        "proxy": False,
        "points": points,
        "spearman_r": round(spearman(rows, x_key, y_key), 3),
        "median_x": round(med_x, 2),
        "median_y": round(med_y, 2),
        "fit_line": [{"x": round(min_x, 2), "y": round(intercept + slope * min_x, 2)}, {"x": round(max_x, 2), "y": round(intercept + slope * max_x, 2)}],
        "fit_r_squared": "",
        "fit_model_type": "ols_linear",
        "above_fit": residuals[-1],
        "below_fit": residuals[0],
    }


def residual_rows(year_rows: list[dict]) -> list[dict]:
    rows = []
    for x_key, y_key, stage in (
        ("diagnosis", "treatment", "Treatment after diagnosis"),
        ("treatment", "suppression", "Suppression after treatment"),
    ):
        slope, intercept = fit_line(year_rows, x_key, y_key)
        for row in year_rows:
            expected = intercept + slope * row[x_key]
            rows.append(
                {
                    "region": row["region"],
                    "stage": stage,
                    "label": f"{row['region']} | {stage}",
                    "value": round(row[y_key] - expected, 2),
                    "observed": round(row[y_key], 1),
                    "expected": round(expected, 1),
                    "source_url": SOURCE_URL,
                    "filename": FILENAME,
                    "period_label": PERIOD,
                }
            )
    return sorted(rows, key=lambda row: row["value"])


def leakage_rows() -> list[dict]:
    rows = []
    for code, alive, ltfu, dead, trans_out, stopped, total, _ in TREATMENT_OUTCOME:
        not_on = total - alive
        rows.append(
            {
                "region": region_label(code),
                "alive": float(alive),
                "ltfu": float(ltfu),
                "not_on_treatment": float(not_on),
                "other_off_treatment_label": "Total not currently receiving ART",
                "is_proxy_off_treatment": False,
                "period_label": PERIOD,
                "source_url": SOURCE_URL,
                "filename": FILENAME,
                "missing_not_on_treatment": False,
            }
        )
    rows.sort(key=lambda row: row["ltfu"] + row["not_on_treatment"], reverse=True)
    return rows


def update_publication_assets() -> None:
    path = NORMALIZED / "publication_assets.json"
    payload = load_json(path)
    series = payload.setdefault("series", {})

    national = series.setdefault("national_cascade", {"rows": [], "estimated_points": []})
    national["estimated_points"] = upsert_annual(national.get("estimated_points", []), annual_point(YEAR, PERIOD, NATIONAL["estimated_plhiv"]))
    stage_updates = {
        "first_95": (NATIONAL["diagnosed_pct"], NATIONAL["diagnosed_plhiv"]),
        "second_95": (NATIONAL["on_art_pct"], NATIONAL["on_art"]),
        "third_95": (NATIONAL["suppressed_pct_of_on_art"], NATIONAL["vl_suppressed"]),
    }
    for row in national.get("rows", []):
        if row.get("series_id") not in stage_updates:
            continue
        pct_value, count_value = stage_updates[row["series_id"]]
        row["points"] = upsert_period_point(row.get("points", []), {"period": PERIOD, "value": float(pct_value)})
        row["official_annual"] = upsert_annual(row.get("official_annual", []), annual_point(YEAR, PERIOD, pct_value))
        row["count_points"] = upsert_annual(row.get("count_points", []), annual_point(YEAR, PERIOD, count_value))
        row["latest_value"] = float(pct_value)
        row["latest_period"] = PERIOD
        row["gap_to_target"] = round(95.0 - float(pct_value), 1)
        row["coverage_end"] = PERIOD
        row["official_context_label"] = "Official HARP checkpoints use year-end values from 2018-2025 plus the March 2026 HASP update."

    publication_rows = regional_rows_for_publication()
    series["regional_ladder"] = {"period_label": PERIOD, "rows": [{k: row[k] for k in ("region", "diagnosis", "treatment", "suppression", "mean_gap")} for row in publication_rows]}

    regional_yearly = series.setdefault("regional_yearly", {})
    years = sorted(set(int(year) for year in regional_yearly.get("years", []) if year) | {YEAR})
    regional_yearly["years"] = years
    regional_yearly["default_year"] = YEAR
    regional_yearly.setdefault("rows_by_year", {})[str(YEAR)] = publication_rows
    regional_yearly["regions"] = sorted(set(regional_yearly.get("regions", [])) | {row["region"] for row in publication_rows})
    histories = regional_yearly.setdefault("region_histories", {})
    for row in publication_rows:
        history = histories.setdefault(row["region"], {"cascade": [], "burden": []})
        history["cascade"] = [old for old in history.get("cascade", []) if int(old.get("year") or 0) != YEAR]
        history["cascade"].append(row)
        history["cascade"].sort(key=lambda item: int(item.get("year") or 0))
    new_case_lookup = {region_label(code): count for code, count, _ in NEW_CASES_REGION}
    for region, count in new_case_lookup.items():
        history = histories.setdefault(region, {"cascade": [], "burden": []})
        history["burden"] = [old for old in history.get("burden", []) if int(old.get("year") or 0) != YEAR]
        history["burden"].append({"year": YEAR, "value": float(count), "period_label": PERIOD, "source_url": SOURCE_URL, "filename": FILENAME})
        history["burden"].sort(key=lambda item: int(item.get("year") or 0))
    national_history = regional_yearly.setdefault("national_history", {"diagnosis": [], "treatment": [], "suppression": []})
    for key, value in (("diagnosis", NATIONAL["diagnosed_pct"]), ("treatment", NATIONAL["on_art_pct"]), ("suppression", NATIONAL["suppressed_pct_of_on_art"])):
        national_history[key] = [old for old in national_history.get(key, []) if int(old.get("year") or 0) != YEAR]
        national_history[key].append({"year": YEAR, "value": float(value), "period_label": PERIOD, "source_url": SOURCE_URL, "filename": FILENAME})
        national_history[key].sort(key=lambda item: int(item.get("year") or 0))
    regional_yearly["coverage_note"] = "Yearly regional cascade uses the latest observed comparable checkpoint inside each year. Structured region-level cascade is currently available for 2024, 2025, and 2026 Q1."

    residuals = residual_rows(publication_rows)
    leakage = leakage_rows()
    residual_by_region = {}
    for row in residuals:
        current = residual_by_region.get(row["region"])
        if current is None or abs(row["value"]) > abs(current["value"]):
            residual_by_region[row["region"]] = row
    performance_burden = []
    for row in leakage:
        residual = residual_by_region.get(row["region"])
        if residual:
            performance_burden.append(
                {
                    "region": row["region"],
                    "residual": residual["value"],
                    "stage": residual["stage"],
                    "alive": row["alive"],
                    "ltfu": row["ltfu"],
                    "not_on_treatment": row["not_on_treatment"],
                    "leakage_burden": row["ltfu"] + row["not_on_treatment"],
                }
            )
    series["anomalies"] = {
        "period_label": PERIOD,
        "residual_rows": sorted(residuals, key=lambda row: abs(row["value"]), reverse=True)[:8],
        "leakage_rows": leakage[:10],
        "performance_burden_rows": performance_burden,
    }
    anomaly_yearly = series.setdefault("anomaly_yearly", {})
    anomaly_yearly["years"] = sorted(set(int(year) for year in anomaly_yearly.get("years", []) if year) | {YEAR})
    anomaly_yearly["default_year"] = YEAR
    anomaly_yearly.setdefault("residuals_by_year", {})[str(YEAR)] = residuals
    anomaly_yearly.setdefault("leakage_by_year", {})[str(YEAR)] = leakage
    anomaly_yearly["coverage_note"] = "Residuals are available where regional cascade coverage exists. Treatment leakage uses the latest official treatment-outcome snapshot inside each year, including 2026 Q1."

    historical = series.setdefault("historical", {})
    historical["cases"] = upsert_annual(historical.get("cases", []), annual_point(YEAR, PERIOD, DIAGNOSIS["cumulative_cases"]))
    historical["sexual_share"] = upsert_annual(historical.get("sexual_share", []), annual_point(YEAR, PERIOD, MOT["cumulative"]["sexual_contact_pct"]))

    key_pop = series.setdefault("key_populations", {})
    key_pop["pregnant_cumulative"] = upsert_annual(key_pop.get("pregnant_cumulative", []), annual_point(YEAR, PERIOD, PREGNANT["cumulative_reported"]))
    key_pop["tgw_cumulative"] = upsert_annual(key_pop.get("tgw_cumulative", []), annual_point(YEAR, PERIOD, TGW["cumulative_diagnosed"]))
    key_pop["ofw_cumulative"] = upsert_annual(key_pop.get("ofw_cumulative", []), annual_point(YEAR, PERIOD, MIGRANT["cumulative_reported"]))
    key_pop["youth_share"] = upsert_annual(key_pop.get("youth_share", []), annual_point(YEAR, PERIOD, CUMULATIVE_AGE["age_15_24"][1]))

    update_experimental(series)
    update_methodology_and_references(payload)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    save_json(path, payload)


def update_experimental(series: dict) -> None:
    exp = series.get("experimental_regional")
    if not exp:
        return
    exp["latest_hard_year"] = YEAR
    exp["latest_observed_year"] = YEAR
    exp["default_year"] = YEAR
    exp["years"] = sorted(set(int(year) for year in exp.get("years", []) if year) | {YEAR})
    exp["coverage_note"] = "Observed annual national seed values now include the March 2026 HASP checkpoint. Observed regional anchors span 2024-2026 Q1; forecasts remain forward-looking beyond the latest hard checkpoint."
    sim = exp.setdefault("simulation", {})
    sim["latest_hard_year"] = YEAR
    sim["regional_anchor_year"] = YEAR
    sim["forecast_start_year"] = YEAR + 1
    sim["forecast_end_year"] = max(int(sim.get("forecast_end_year") or 2035), 2035)
    exp["forecast_end_year"] = sim["forecast_end_year"]
    observed = exp.setdefault("national", {}).setdefault("observed", [])
    observed = [row for row in observed if int(row.get("year") or 0) != YEAR]
    estimated = float(NATIONAL["estimated_plhiv"])
    diagnosed = float(NATIONAL["diagnosed_plhiv"])
    on_art = float(NATIONAL["on_art"])
    suppressed = float(NATIONAL["vl_suppressed"])
    observed.append(
        {
            "year": YEAR,
            "diagnosis": float(NATIONAL["diagnosed_pct"]),
            "treatment": float(NATIONAL["on_art_pct"]),
            "suppression": float(NATIONAL["suppressed_pct_of_on_art"]),
            "estimated_plhiv": estimated,
            "diagnosed_plhiv": diagnosed,
            "plhiv_on_art": on_art,
            "suppressed_count": suppressed,
            "diagnosis_gap_count": max(estimated - diagnosed, 0.0),
            "treatment_gap_count": max(diagnosed - on_art, 0.0),
            "suppression_gap_count": max(on_art - suppressed, 0.0),
        }
    )
    exp["national"]["observed"] = sorted(observed, key=lambda row: int(row.get("year") or 0))
    exp["national"]["forecast"] = [row for row in exp["national"].get("forecast", []) if int(row.get("year") or 0) > YEAR]
    exp["national"]["forecast_years"] = [year for year in exp["national"].get("forecast_years", []) if int(year) > YEAR]
    observed_values = {
        "diagnosis": float(NATIONAL["diagnosed_pct"]),
        "treatment": float(NATIONAL["on_art_pct"]),
        "suppression": float(NATIONAL["suppressed_pct_of_on_art"]),
    }
    for stage, paths in exp["national"].get("paths", {}).get("stages", {}).items():
        value = observed_values.get(stage)
        if value is None:
            continue
        for path in paths:
            for idx, year in enumerate(path.get("years", [])):
                if int(year) == YEAR and idx < len(path.get("values", [])):
                    path["values"][idx] = round(value, 1)
    for stage in ("diagnosis", "treatment", "suppression"):
        exp.setdefault("observed_years_by_stage", {}).setdefault(stage, [])
        exp["observed_years_by_stage"][stage] = sorted(set(exp["observed_years_by_stage"][stage]) | {YEAR})
        exp.setdefault("forecast_years_by_stage", {}).setdefault(stage, [])
        exp["forecast_years_by_stage"][stage] = [year for year in exp["forecast_years_by_stage"][stage] if int(year) > YEAR]

    publication_rows = regional_rows_for_publication()
    exp.setdefault("rows_by_year", {})[str(YEAR)] = []
    regional_histories = exp.setdefault("regional", {}).setdefault("region_histories", {})
    exp.setdefault("region_histories", regional_histories)
    leakage_lookup = {row["region"]: row for row in leakage_rows()}
    for row in publication_rows:
        exp_row = {
            "year": YEAR,
            "region": row["region"],
            "diagnosis": row["diagnosis"],
            "diagnosis_status": "observed",
            "diagnosis_lower": row["diagnosis"],
            "diagnosis_upper": row["diagnosis"],
            "diagnosis_period": PERIOD,
            "diagnosis_source_url": SOURCE_URL,
            "diagnosis_filename": FILENAME,
            "treatment": row["treatment"],
            "treatment_status": "observed",
            "treatment_lower": row["treatment"],
            "treatment_upper": row["treatment"],
            "treatment_period": PERIOD,
            "treatment_source_url": SOURCE_URL,
            "treatment_filename": FILENAME,
            "suppression": row["suppression"],
            "suppression_status": "observed",
            "suppression_lower": row["suppression"],
            "suppression_upper": row["suppression"],
            "suppression_period": PERIOD,
            "suppression_source_url": SOURCE_URL,
            "suppression_filename": FILENAME,
            "leakage_burden": None,
            "leakage_status": "missing",
        }
        leak = leakage_lookup.get(row["region"])
        if leak:
            exp_row["leakage_burden"] = leak["ltfu"] + leak["not_on_treatment"]
            exp_row["leakage_status"] = "observed"
        exp["rows_by_year"][str(YEAR)].append(exp_row)
        history = regional_histories.setdefault(row["region"], {"cascade": [], "paths": {}, "model": {}})
        history["cascade"] = [old for old in history.get("cascade", []) if int(old.get("year") or 0) != YEAR]
        history["cascade"].append(exp_row)
        history["cascade"].sort(key=lambda item: int(item.get("year") or 0))
        for stage in ("diagnosis", "treatment", "suppression"):
            for path in history.get("paths", {}).get(stage, []):
                for idx, year in enumerate(path.get("years", [])):
                    if int(year) == YEAR and idx < len(path.get("values", [])):
                        path["values"][idx] = round(float(exp_row[stage]), 1)
            model = history.get("model", {}).setdefault(stage, {})
            model["latest_anchor_year"] = YEAR
            model["anchor_years"] = sorted(set(model.get("anchor_years", [])) | {YEAR})
    exp["rows_by_year"][str(YEAR)].sort(
        key=lambda row: ((95 - row["diagnosis"]) + (95 - row["treatment"]) + (95 - row["suppression"]))
    )
    exp["regions"] = sorted(set(exp.get("regions", [])) | {row["region"] for row in publication_rows})


def update_methodology_and_references(payload: dict) -> None:
    refs = payload.setdefault("references", {})
    items = refs.setdefault("items", [])
    items[:] = [item for item in items if item.get("id") != "hasp-2026-q1"]
    items.append(
        {
            "id": "hasp-2026-q1",
            "title": "2026 Q1 HIV & AIDS Surveillance of the Philippines",
            "organization": "Department of Health Epidemiology Bureau / HARP",
            "kind": "Official quarterly surveillance report",
            "url": SOURCE_URL,
            "used_in": ["national_cascade", "regional_ladder", "anomaly_board", "historical_board", "key_populations_board"],
            "note": "User-provided latest HASP report. Extracted into dist/data/normalized/hasp_2026_q1_extracted.json and used as the March 2026 checkpoint.",
        }
    )
    groups = refs.setdefault("groups", [])
    group = next((entry for entry in groups if entry.get("title") == "Official Philippines surveillance reports"), None)
    if group is None:
        groups.append({"title": "Official Philippines surveillance reports", "item_ids": ["hasp-2026-q1"]})
    else:
        ids = [item for item in group.get("item_ids", []) if item != "hasp-2026-q1"]
        ids.append("hasp-2026-q1")
        group["item_ids"] = ids

    methodology = payload.setdefault("methodology", {})
    for section in methodology.get("sections", []):
        refs_for_section = section.setdefault("reference_ids", [])
        if "hasp-2026-q1" not in refs_for_section:
            refs_for_section.append("hasp-2026-q1")
        sid = section.get("id")
        if sid == "national_cascade":
            section["coverage_window"] = "Published HARP checkpoints: 2018-2025 year-end values plus the March 2026 HASP update."
            section["construction"] = ["Use the official HARP annual table through 2025.", "Append the 2026 Q1 HASP national cascade: 55% diagnosed, 69% on ART, and 55% virally suppressed among PLHIV on ART."]
        elif sid == "regional_ladder":
            section["coverage_window"] = "Regional cascade coverage currently spans 2024-2026 Q1, with March 2026 values extracted from the report annex."
        elif sid == "anomaly_board":
            section["coverage_window"] = "Anomaly and leakage views use the latest available regional cascade and treatment-outcome checkpoint, now March 2026."
        elif sid == "historical_board":
            section["coverage_window"] = "Observed DOH/HARP cumulative case and subgroup checkpoints now include 2026 Q1 where the report provides values."
        elif sid == "key_populations_board":
            section["coverage_window"] = "Key-population sentinel panels include 2026 Q1 HASP updates for pregnant women, TGW, OFW/migrant workers, and youth share where reported."

    suffix = "Latest interactive dashboard data include the March 2026 HASP checkpoint."
    for figure_id, figure in payload.get("figures", {}).items():
        note = re.sub(r"(?:\s*Latest interactive dashboard data include the March 2026 HASP checkpoint\.)+", "", figure.get("note", "")).strip()
        if figure_id == "national_cascade":
            note = "The board combines a compact target-position strip, observed 2018-2025 year-end trajectories from the official DOH/HARP table, and the observed 2026 Q1 stage counts."
        figure["note"] = f"{note} {suffix}".strip()
        # The generated static figure exports in this repository predate the
        # 2026 Q1 report. Clear their paths so the UI does not offer stale
        # PDF/SVG/PNG downloads while the interactive charts use current data.
        for key in ("svg", "svg_path", "png_path", "figure_pdf_path", "pdf_path"):
            figure[key] = ""


def snapshot(metric_type: str, value: float, unit: str = "count", **kwargs) -> dict:
    return make_obs(metric_type, value, unit, "snapshot", "quarter", PERIOD, **kwargs)


def cumulative(metric_type: str, value: float, unit: str = "count", period_label: str = f"1984-{PERIOD}", **kwargs) -> dict:
    return make_obs(metric_type, value, unit, "cumulative", "cumulative", period_label, **kwargs)


def make_obs(
    metric_type: str,
    value: float,
    unit: str,
    period_scope: str,
    period_granularity: str,
    period_label: str,
    *,
    region: str = "Philippines",
    subgroup: str = "",
    page_index: int = 0,
    snippet: str = "",
) -> dict:
    make_obs.counter += 1
    return {
        "observation_id": f"hasp-2026-q1:{make_obs.counter:04d}:{metric_type}:{region}:{subgroup}:{period_scope}:{period_label}",
        "claim_id": 900000 + make_obs.counter,
        "document_id": 202601,
        "folder": "hiv_sti",
        "filename": FILENAME,
        "document_type": "surveillance_report",
        "primary_disease": "HIV",
        "metric_type": metric_type,
        "region": region,
        "subgroup": subgroup,
        "year": YEAR,
        "quarter": QUARTER,
        "month": "",
        "period_granularity": period_granularity,
        "period_scope": period_scope,
        "period_label": period_label,
        "value": value,
        "unit": unit,
        "confidence": 0.98,
        "source_url": SOURCE_URL,
        "page_index": page_index,
        "snippet": snippet or f"Extracted from {FILENAME}, {PERIOD}.",
    }


make_obs.counter = 0


def build_observations() -> list[dict]:
    obs: list[dict] = []
    obs.extend(
        [
            snapshot("estimated_plhiv", NATIONAL["estimated_plhiv"], page_index=0),
            snapshot("diagnosed_plhiv_count", NATIONAL["diagnosed_plhiv"], page_index=0),
            snapshot("diagnosed_plhiv_pct", NATIONAL["diagnosed_pct"], "percent", page_index=0),
            snapshot("plhiv_on_art_count", NATIONAL["on_art"], page_index=0),
            snapshot("plhiv_on_art_pct", NATIONAL["on_art_pct"], "percent", page_index=0),
            snapshot("viral_load_tested_count", NATIONAL["vl_tested"], page_index=0),
            snapshot("viral_load_tested_pct", NATIONAL["vl_tested_pct_of_on_art"], "percent", page_index=0),
            snapshot("vl_testing_coverage", NATIONAL["vl_tested_pct_of_eligible"], "percent", page_index=3),
            snapshot("viral_load_suppressed_count", NATIONAL["vl_suppressed"], page_index=0),
            snapshot("viral_load_suppressed_pct", NATIONAL["vl_suppressed_pct_of_tested"], "percent", page_index=0),
            snapshot("suppression_among_on_art_pct", NATIONAL["suppressed_pct_of_on_art"], "percent", page_index=0),
            snapshot("viral_load_unsuppressed_count", NATIONAL["vl_unsuppressed"], page_index=4),
            snapshot("viral_load_eligible", NATIONAL["eligible_for_vl"], page_index=3),
        ]
    )
    obs.extend(
        [
            snapshot("prep_new_enrollees_count", 8229, page_index=0),
            cumulative("prep_enrolled_count", 96890, period_label="2021-2026 Q1", page_index=0),
            cumulative("prep_male_count", 92691, period_label="2021-2026 Q1", page_index=0),
            cumulative("prep_male_pct", 96, "percent", period_label="2021-2026 Q1", page_index=0),
            cumulative("prep_age_25_plus_count", 61517, period_label="2021-2026 Q1", page_index=0),
            cumulative("prep_age_25_plus_pct", 63, "percent", period_label="2021-2026 Q1", page_index=0),
            snapshot("prep_refill_count", 14614, page_index=0),
            snapshot("prep_refill_pct", 24, "percent", page_index=0),
            snapshot("prep_non_returnees_count", 65838, page_index=0),
            snapshot("prep_non_returnee_hiv_positive_count", 2012, page_index=0),
        ]
    )
    for subgroup, (count, pct) in {
        "age_under_18": (96, 3),
        "age_18_24": (3385, 41),
        "age_25_34": (3557, 43),
        "age_35_plus": (1173, 14),
    }.items():
        obs.append(snapshot("prep_new_enrollees_count", count, subgroup=subgroup, page_index=0))
        obs.append(snapshot("prep_new_enrollees_pct", pct, "percent", subgroup=subgroup, page_index=0))
    for region, count, pct in (("NCR", 3457, 42), ("4A", 1389, 17), ("7", 914, 11)):
        obs.append(snapshot("prep_new_enrollees_count", count, region=region, page_index=0))
        obs.append(snapshot("prep_new_enrollees_pct", pct, "percent", region=region, page_index=0))

    obs.extend(
        [
            snapshot("new_cases_count", DIAGNOSIS["new_cases"], page_index=0),
            snapshot("new_cases_male_count", DIAGNOSIS["new_cases_male"], page_index=0),
            snapshot("new_cases_female_count", DIAGNOSIS["new_cases_female"], page_index=0),
            snapshot("new_cases_male_pct", DIAGNOSIS["new_cases_male_pct"], "percent", page_index=0),
            snapshot("new_cases_female_pct", DIAGNOSIS["new_cases_female_pct"], "percent", page_index=0),
            snapshot("advanced_hiv_disease_count", DIAGNOSIS["advanced_hiv_disease"], page_index=0),
            snapshot("advanced_hiv_disease_pct", DIAGNOSIS["advanced_hiv_disease_pct"], "percent", page_index=0),
            snapshot("average_cases_per_day", DIAGNOSIS["average_cases_per_day"], page_index=0),
            cumulative("reported_cases_count", DIAGNOSIS["cumulative_cases"], page_index=1),
            cumulative("reported_cases_count", DIAGNOSIS["cumulative_male"], subgroup="male", page_index=1),
            cumulative("reported_cases_pct", DIAGNOSIS["cumulative_male_pct"], "percent", subgroup="male", page_index=1),
            cumulative("reported_cases_count", DIAGNOSIS["cumulative_female"], subgroup="female", page_index=1),
            cumulative("reported_cases_pct", DIAGNOSIS["cumulative_female_pct"], "percent", subgroup="female", page_index=1),
        ]
    )
    for subgroup, (count, pct) in NEW_CASES_AGE.items():
        obs.append(snapshot("new_cases_count", count, subgroup=subgroup, page_index=0))
        obs.append(snapshot("new_cases_pct", pct, "percent", subgroup=subgroup, page_index=0))
    for subgroup, (count, pct) in CUMULATIVE_AGE.items():
        obs.append(cumulative("reported_cases_count", count, subgroup=subgroup, page_index=1))
        obs.append(cumulative("reported_cases_pct", pct, "percent", subgroup=subgroup, page_index=1))
    for region, count, pct in NEW_CASES_REGION:
        obs.append(snapshot("new_cases_count", count, region=region, page_index=1))
        obs.append(snapshot("new_cases_pct", pct, "percent", region=region, page_index=1))

    c = MOT["cumulative"]
    obs.extend(
        [
            cumulative("sexual_contact_cases_count", c["sexual_contact"], page_index=2),
            cumulative("sexual_contact_cases_pct", c["sexual_contact_pct"], "percent", page_index=2),
            cumulative("male_male_sex_cases_count", c["male_male"], page_index=2),
            cumulative("male_male_female_sex_cases_count", c["male_male_female"], page_index=2),
            cumulative("male_female_sex_cases_count", c["male_female"], page_index=2),
            cumulative("needle_transmission_count", c["needle"], page_index=2),
            cumulative("needle_transmission_pct", c["needle_pct"], "percent", page_index=2),
            cumulative("mother_to_child_transmission_count", c["mother_to_child"], page_index=2),
            cumulative("blood_transmission_count", c["blood_products"], page_index=2),
            cumulative("needle_prick_injury_transmission_count", c["needlestick"], page_index=2),
            snapshot("new_cases_sexual_contact_count", MOT["q1"]["sexual_contact"][0], page_index=2),
            snapshot("new_cases_sexual_contact_pct", MOT["q1"]["sexual_contact"][1], "percent", page_index=2),
            snapshot("new_cases_male_male_sex_count", MOT["q1"]["male_male"], page_index=2),
            snapshot("new_cases_male_male_female_sex_count", MOT["q1"]["male_male_female"], page_index=2),
            snapshot("new_cases_male_female_sex_count", MOT["q1"]["male_female"], page_index=2),
            snapshot("needle_transmission_count", MOT["q1"]["needle"], page_index=2),
            snapshot("mother_to_child_transmission_count", MOT["q1"]["mother_to_child"], page_index=2),
        ]
    )

    obs.extend(
        [
            cumulative("advanced_hiv_disease_count", AHD["cumulative_count"], page_index=2),
            cumulative("advanced_hiv_disease_pct", AHD["cumulative_pct"], "percent", page_index=2),
            snapshot("newly_enrolled_art_count", ART["newly_enrolled"], page_index=3),
            snapshot("median_baseline_cd4", ART["median_baseline_cd4"], "cells/mm3", page_index=3),
            snapshot("ever_enrolled_art_count", ART["ever_enrolled_total"], page_index=3),
            snapshot("lost_to_follow_up_count", ART["lost_to_follow_up"], page_index=3),
            snapshot("not_on_treatment_count", ART["no_longer_receiving"], page_index=3),
            snapshot("reported_deaths_count", MORTALITY["new_deaths"], page_index=4),
            cumulative("reported_deaths_count", MORTALITY["cumulative_deaths"], page_index=4),
        ]
    )
    for code, alive, ltfu, dead, trans_out, stopped, total, pct_ltfu in TREATMENT_OUTCOME:
        not_on = total - alive
        obs.extend(
            [
                snapshot("alive_on_art_count", alive, region=code, page_index=3),
                snapshot("lost_to_follow_up_count", ltfu, region=code, page_index=3),
                snapshot("reported_deaths_count", dead, region=code, page_index=3),
                snapshot("transferred_overseas_count", trans_out, region=code, page_index=3),
                snapshot("refused_art_count", stopped, region=code, page_index=3),
                snapshot("ever_enrolled_art_count", total, region=code, page_index=3),
                snapshot("lost_to_follow_up_pct", pct_ltfu, "percent", region=code, page_index=3),
                snapshot("not_on_treatment_count", not_on, region=code, page_index=3),
            ]
        )
    for code, est, dx, dx_pct, on_art, art_pct, vl_tested, vl_cov, vl_supp, vl_supp_tested, third in REGIONAL_CASCADE:
        obs.extend(
            [
                snapshot("estimated_plhiv", est, region=code, page_index=6),
                snapshot("diagnosed_plhiv", dx, region=code, page_index=6),
                snapshot("diagnosis_coverage", dx_pct, "percent", region=code, page_index=6),
                snapshot("on_art", on_art, region=code, page_index=6),
                snapshot("art_coverage", art_pct, "percent", region=code, page_index=6),
                snapshot("viral_load_tested_count", vl_tested, region=code, page_index=6),
                snapshot("vl_testing_coverage", vl_cov, "percent", region=code, page_index=6),
                snapshot("viral_load_suppressed_count", vl_supp, region=code, page_index=6),
                snapshot("viral_load_suppressed_pct", vl_supp_tested, "percent", region=code, page_index=6),
                snapshot("suppression_among_on_art", third, "percent", region=code, page_index=6),
            ]
        )
    for subgroup, est, dx, dx_pct, on_art, art_pct, vl_tested, vl_cov, vl_supp, vl_supp_tested, third in AGE_CASCADE + KEYPOP_CASCADE:
        obs.extend(
            [
                snapshot("estimated_plhiv", est, subgroup=subgroup, page_index=6),
                snapshot("diagnosed_plhiv", dx, subgroup=subgroup, page_index=6),
                snapshot("diagnosis_coverage", dx_pct, "percent", subgroup=subgroup, page_index=6),
                snapshot("on_art", on_art, subgroup=subgroup, page_index=6),
                snapshot("art_coverage", art_pct, "percent", subgroup=subgroup, page_index=6),
                snapshot("viral_load_tested_count", vl_tested, subgroup=subgroup, page_index=6),
                snapshot("vl_testing_coverage", vl_cov, "percent", subgroup=subgroup, page_index=6),
                snapshot("viral_load_suppressed_count", vl_supp, subgroup=subgroup, page_index=6),
                snapshot("viral_load_suppressed_pct", vl_supp_tested, "percent", subgroup=subgroup, page_index=6),
                snapshot("suppression_among_on_art", third, "percent", subgroup=subgroup, page_index=6),
            ]
        )

    obs.extend(
        [
            snapshot("pregnant_women_reported_count", PREGNANT["q1_reported"], subgroup="pregnant_women", page_index=4),
            cumulative("pregnant_women_reported_count", PREGNANT["cumulative_reported"], subgroup="pregnant_women", period_label="2011-2026 Q1", page_index=4),
            snapshot("tgw_diagnosed_count", TGW["q1_reported"], subgroup="transgender_women", page_index=4),
            cumulative("tgw_diagnosed_count", TGW["cumulative_diagnosed"], subgroup="transgender_women", period_label="2018-2026 Q1", page_index=4),
            cumulative("reported_cases_count", MIGRANT["cumulative_reported"], subgroup="migrant_workers", page_index=5),
            snapshot("reported_cases_count", MIGRANT["q1_reported"], subgroup="migrant_workers", page_index=5),
            snapshot("reported_cases_count", TRANSACTIONAL["q1_reported"], subgroup="transactional_sex", page_index=5),
            cumulative("reported_cases_count", TRANSACTIONAL["cumulative_reported"], subgroup="transactional_sex", period_label="2012-2026 Q1", page_index=5),
        ]
    )
    return obs


def update_observations_and_summary() -> None:
    observations_path = NORMALIZED / "observations.jsonl"
    existing = []
    if observations_path.exists():
        for line in observations_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not str(row.get("observation_id", "")).startswith("hasp-2026-q1:"):
                existing.append(row)
    new_obs = build_observations()
    with observations_path.open("w", encoding="utf-8") as handle:
        for row in existing + new_obs:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary_path = NORMALIZED / "summary.json"
    summary = load_json(summary_path)
    summary["observations_count"] = len(existing) + len(new_obs)
    save_json(summary_path, summary)

    dashboard_path = NORMALIZED / "dashboard_feed.json"
    dashboard = load_json(dashboard_path)
    dashboard.setdefault("kpis", {})["observations"] = len(existing) + len(new_obs)
    update_dashboard(dashboard)
    save_json(dashboard_path, dashboard)


def update_dashboard(dashboard: dict) -> None:
    charts = dashboard.setdefault("charts", {})
    for collection_name in ("national_cascade", "national_goal_board"):
        for row in charts.get(collection_name, []):
            sid = row.get("series_id")
            value_count = {
                "first_95": (NATIONAL["diagnosed_pct"], NATIONAL["diagnosed_plhiv"]),
                "second_95": (NATIONAL["on_art_pct"], NATIONAL["on_art"]),
                "third_95": (NATIONAL["suppressed_pct_of_on_art"], NATIONAL["vl_suppressed"]),
            }.get(sid)
            if not value_count:
                continue
            value, count = value_count
            row["percent_points"] = upsert_period_point(row.get("percent_points", []), point(PERIOD, value))
            row["count_points"] = upsert_period_point(row.get("count_points", []), point(PERIOD, count))
            row["coverage_end"] = PERIOD
            row["coverage_point_count"] = len(row.get("percent_points", []))
            row["latest_period"] = PERIOD
            row["latest_value"] = float(value)
            row["gap_to_target"] = round(95.0 - float(value), 1)
    for row in charts.get("goal_forecasts", []):
        sid = row.get("series_id")
        value = {"first_95": 55, "second_95": 69, "third_95": 55}.get(sid)
        if value is None:
            continue
        row["points"] = upsert_period_point(row.get("points", []), point(PERIOD, value))
        row["point_count"] = len(row.get("points", []))
        row["latest_period"] = PERIOD
        row["latest_value"] = float(value)
        row["gap_to_target"] = round(95.0 - float(value), 1)
        row["deadline_status"] = "missed"
        row["trajectory"] = "volatile"

    regional = regional_rows_for_dashboard()
    regional_gaps = []
    for snapshot_id, label, metric, key in (
        ("regional_diagnosis_gap", "Diagnosis coverage by region", "diagnosis_coverage", "diagnosis"),
        ("regional_treatment_gap", "Treatment coverage by region", "art_coverage", "treatment"),
        ("regional_suppression_gap", "Viral suppression by region", "suppression_among_on_art", "suppression"),
    ):
        rows = [
            {"region": row["region"], "value": row[key], "gap_to_target": round(95.0 - row[key], 2)}
            for row in regional
        ]
        rows.sort(key=lambda row: (-row["value"], row["region"]))
        regional_gaps.append(
            {
                "snapshot_id": snapshot_id,
                "label": label,
                "metric_type": metric,
                "proxy": False,
                "note": "",
                "period_label": PERIOD,
                "target_value": 95.0,
                "unit": "percent",
                "spread": round(rows[0]["value"] - rows[-1]["value"], 2),
                "median_value": round(median([row["value"] for row in rows]), 2),
                "leader_region": rows[0]["region"],
                "laggard_region": rows[-1]["region"],
                "regions": rows,
            }
        )
    charts["regional_gaps"] = regional_gaps

    score_rows = []
    for row in regional:
        gaps = {
            "diagnosis_coverage": round(95.0 - row["diagnosis"], 2),
            "treatment_coverage": round(95.0 - row["treatment"], 2),
            "viral_suppression": round(95.0 - row["suppression"], 2),
        }
        values = {
            "diagnosis_coverage": row["diagnosis"],
            "treatment_coverage": row["treatment"],
            "viral_suppression": row["suppression"],
        }
        score_rows.append(
            {
                "region": row["region"],
                "metric_count": 3,
                "mean_gap_to_target": round(sum(gaps.values()) / 3.0, 2),
                "values": values,
                "gaps": gaps,
            }
        )
    score_rows.sort(key=lambda row: (row["mean_gap_to_target"], row["region"]))
    charts["regional_scorecard"] = {
        "periods": {"diagnosis_coverage": PERIOD, "treatment_coverage": PERIOD, "viral_suppression": PERIOD},
        "rows": score_rows,
        "top_performing": score_rows[:5],
        "largest_gaps": sorted(score_rows, key=lambda row: (-row["mean_gap_to_target"], row["region"]))[:5],
    }
    charts["relationship_scatter"] = [
        relationship_chart("diagnosis_vs_treatment", "Diagnosis vs treatment coverage", "diagnosis", "treatment", "Diagnosis coverage", "Treatment coverage"),
        relationship_chart("treatment_vs_suppression", "Treatment vs viral suppression", "treatment", "suppression", "Treatment coverage", "Viral suppression"),
    ]

    update_panel(charts.get("burden_views", []), "cumulative_reported_cases", "1984-2026 Q1", DIAGNOSIS["cumulative_cases"])
    update_panel(charts.get("burden_views", []), "plhiv_on_art", PERIOD, NATIONAL["on_art"])
    update_panel(charts.get("burden_views", []), "reported_deaths", PERIOD, MORTALITY["new_deaths"])
    update_panel(charts.get("transmission_views", []), "sexual_contact_share", "1984-2026 Q1", MOT["cumulative"]["sexual_contact_pct"])
    update_panel(charts.get("transmission_views", []), "mother_to_child_transmission", "1984-2026 Q1", MOT["cumulative"]["mother_to_child"])
    update_panel(charts.get("transmission_views", []), "needle_transmission", "1984-2026 Q1", MOT["cumulative"]["needle"])

    dashboard["highlights"] = [
        {"title": "95-95-95 status", "detail": "Latest national cascade readout: first 95 55.0%, second 95 69.0%, third 95 55.0% at 2026 Q1."},
        {"title": "Diagnosis coverage by region", "detail": f"{regional_gaps[0]['leader_region']} leads at {regional_gaps[0]['regions'][0]['value']:.1f}%, while {regional_gaps[0]['laggard_region']} trails in {PERIOD}."},
        {"title": "Treatment coverage by region", "detail": f"{regional_gaps[1]['leader_region']} leads at {regional_gaps[1]['regions'][0]['value']:.1f}%, while {regional_gaps[1]['laggard_region']} trails in {PERIOD}."},
        {"title": "PrEP enrollment", "detail": "New PrEP enrollment reached 8,229 clients in Jan-Mar 2026, 16% above the previous quarter."},
        {"title": "Viral-load testing", "detail": "61,413 PLHIV on ART were tested for viral load in the past 12 months; 59,540 were virally suppressed."},
    ]
    dashboard.setdefault("analytics", {})["relationship_highlights"] = [
        {
            "title": charts["relationship_scatter"][0]["label"],
            "detail": f"{PERIOD} shows Spearman r={charts['relationship_scatter'][0]['spearman_r']:.2f} for diagnosis versus treatment coverage.",
        },
        {
            "title": charts["relationship_scatter"][1]["label"],
            "detail": f"{PERIOD} shows Spearman r={charts['relationship_scatter'][1]['spearman_r']:.2f} for treatment versus suppression.",
        },
    ]


def update_panel(panels: list[dict], panel_id: str, period: str, value: float) -> None:
    panel = next((row for row in panels if row.get("panel_id") == panel_id), None)
    if not panel:
        return
    panel["points"] = upsert_period_point(panel.get("points", []), point(period, value))
    panel["coverage_end"] = period
    panel["point_count"] = len(panel.get("points", []))
    panel["latest_value"] = float(value)
    panel["latest_period"] = period
    if len(panel["points"]) >= 2:
        panel["delta"] = round(float(panel["points"][-1]["value"]) - float(panel["points"][-2]["value"]), 2)


def update_html(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if '<link rel="icon" href="data:,">' not in text:
        text = text.replace("<title>EpiGraph PH — Philippines HIV Surveillance Atlas</title>", "<title>EpiGraph PH — Philippines HIV Surveillance Atlas</title>\n  <link rel=\"icon\" href=\"data:,\">")
    replacements = {
        "Official year-end cascade values use the DOH/HARP accomplishment table from 2018 through 2025. The board shows target position, observed year-end movement, and the 2025 year-end stage counts directly.": "Official HARP cascade checkpoints use year-end values from 2018 through 2025 plus the March 2026 HASP update. The board shows target position, observed movement, and the 2026 Q1 stage counts directly.",
        "<h3>PrEP Coverage</h3>": "<h3>Quarterly PrEP Enrollment</h3>",
        "People receiving pre-exposure prophylaxis. Source: UNAIDS GAM 2025.": "Quarterly newly enrolled PrEP clients from the DOH/HASP surveillance report. Source: 2026 Q1 HASP.",
        "Official DOH/HARP national accomplishment values from 2018 through 2025.": "Official DOH/HARP checkpoint values from 2018 through 2026 Q1.",
        "Official year-end target board": "Official checkpoint target board",
        "2025 year-end cascade counts": "2026 Q1 cascade counts",
        "Counts use the official 2025 year-end HARP table. The third 95 is virally suppressed among PLHIV on ART.": "Counts use the official 2026 Q1 HASP report. The third 95 is virally suppressed among PLHIV on ART.",
        'document.getElementById("historical-atlas-pill").textContent = "2015-2025 series";': 'document.getElementById("historical-atlas-pill").textContent = "2015-2026 series";',
        'document.getElementById("population-outcomes-pill").textContent = `${panels.length} series · 2015-2025`;': 'document.getElementById("population-outcomes-pill").textContent = `${panels.length} series · 2015-2026`;',
        "latest hard official annual year": "latest hard official checkpoint",
        "latest hard official annual": "latest hard official checkpoint",
        "official annual year": "official checkpoint",
        "final verified data point from": "final verified checkpoint from",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = text.replace("x: 2025.1,\n        y: 95,", "x: 2026.15,\n        y: 95,")
    text = text.replace("range: [2017.0, 2026.5],\n            tickvals: [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025],\n            ticktext: [\"2018\", \"2019\", \"2020\", \"2021\", \"2022\", \"2023\", \"2024\", \"2025\"],", "range: [2017.0, 2026.7],\n            tickvals: [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026],\n            ticktext: [\"2018\", \"2019\", \"2020\", \"2021\", \"2022\", \"2023\", \"2024\", \"2025\", \"2026 Q1\"],")
    text = text.replace("x1: 2025.4,\n            y0: 95,", "x1: 2026.35,\n            y0: 95,")
    text = text.replace(
        '          showarrow: true,\n          arrowhead: 0,\n          arrowsize: 0.5,\n          arrowwidth: 1,\n          arrowcolor: meta.color,\n          ax: 55,\n          ay: meta.stageKey === "treatment" ? -45 : (meta.stageKey === "diagnosis" ? 0 : 45),\n          xanchor: "left",',
        '          showarrow: false,\n          xshift: 18,\n          yshift: meta.stageKey === "treatment" ? -18 : (meta.stageKey === "diagnosis" ? 0 : 18),\n          xanchor: "left",',
    )
    text = text.replace(
        'C ${spoutEnd} - 0.1 ${transitionBottom}, ${nextX1 + 0.1} ${transitionBottom}, ${nextX1} ${transitionBottom} Z',
        'C ${spoutEnd - 0.1} ${transitionBottom}, ${nextX1 + 0.1} ${transitionBottom}, ${nextX1} ${transitionBottom} Z',
    )
    text = text.replace(
        "Four key metrics from the UNAIDS national estimates, embedded as interactive charts from the AIDSINFO portal. Use the toolbar on each chart to download or zoom. Source: UNAIDS Estimates 2025.",
        "Key metrics from UNAIDS national estimates rendered locally from packaged AIDSINFO series, with portal links retained for source review. Source: UNAIDS Estimates 2025.",
    )
    iframe_hosts = {
        "People Living with HIV": "unaids-plhiv",
        "New HIV Infections": "unaids-new-infections",
        "AIDS-Related Deaths": "unaids-aids-deaths",
        "Treatment Cascade": "unaids-cascade95",
    }
    for title, host_id in iframe_hosts.items():
        text = re.sub(
            rf'<iframe src="https://aidsinfo\.unaids\.org[^"]+" style="[^"]*" loading="lazy" title="{re.escape(title)}"></iframe>',
            f'<div class="chart-host" id="{host_id}" style="height: 280px;"></div>',
            text,
        )
    text = text.replace(
        'const annualWindow = annual.length\n        ? `${annual[0].year}-${annual[annual.length - 1].year} annual context`\n        : "Quarterly context only";',
        'const firstAnnual = annual[0]?.label || annual[0]?.year;\n      const lastAnnual = annual[annual.length - 1]?.label || annual[annual.length - 1]?.year;\n      const annualWindow = annual.length\n        ? `${firstAnnual}-${lastAnnual} context`\n        : "Quarterly context only";',
    )
    text = text.replace(
        'x: annual.map(point => Number(point.year)),\n          y: annual.map(point => Number(point.value || 0)),',
        'x: annual.map(point => Number(point.year)),\n          y: annual.map(point => Number(point.value || 0)),\n          customdata: annual.map(point => point.label || String(point.year || "")),',
    )
    text = re.sub(
        r"(\n\s*customdata: annual\.map\(point => point\.label \|\| String\(point\.year \|\| \"\"\)\),)+",
        '\n          customdata: annual.map(point => point.label || String(point.year || "")),',
        text,
    )
    text = text.replace(
        '"Official DOH/HARP year-end context<br>" +\n            "Year: %{x:.0f} year-end<br>" +',
        '"Official DOH/HARP checkpoint<br>" +\n            "Checkpoint: %{customdata}<br>" +',
    )
    text = text.replace(
        "      const mapping = {\n        // Prevention tab\n        'unaids-condoms': '10_condom_distribution',\n        'unaids-prep': '11_prep_coverage',\n        'unaids-expenditure-total': '12a_expenditure_total',\n        'unaids-expenditure-domestic': '12b_expenditure_domestic',\n        'unaids-expenditure-international': '12c_expenditure_international',\n        'unaids-expenditure-global-fund': '12d_expenditure_global_fund',\n        // Overview tab — decomposed PMTCT\n        'overview-pmtct-need': '07a_pmtct_need_arv',\n        'overview-pmtct-rate': '07b_pmtct_mtct_rate',\n      };\n      Object.entries(mapping).forEach(([elId, chartKey]) => {\n        const el = document.getElementById(elId);\n        const opt = UNAIDS_CHARTS[chartKey];\n        if (!el) { console.warn('Element not found:', elId); return; }\n        if (!opt) { console.warn('Chart config missing:', chartKey); return; }\n",
        "      const mapping = {\n        // Prevention tab\n        'unaids-plhiv': { chartKey: '01_epidemic_curve', seriesIndex: 0, yName: 'PLHIV' },\n        'unaids-new-infections': { chartKey: '01_epidemic_curve', seriesIndex: 1, yName: 'New infections' },\n        'unaids-aids-deaths': { chartKey: '01_epidemic_curve', seriesIndex: 2, yName: 'AIDS deaths' },\n        'unaids-cascade95': { chartKey: '02_cascade_95' },\n        'unaids-condoms': { chartKey: '10_condom_distribution' },\n        'unaids-prep': { chartKey: '11_prep_coverage' },\n        'unaids-expenditure-total': { chartKey: '12a_expenditure_total' },\n        'unaids-expenditure-domestic': { chartKey: '12b_expenditure_domestic' },\n        'unaids-expenditure-international': { chartKey: '12c_expenditure_international' },\n        'unaids-expenditure-global-fund': { chartKey: '12d_expenditure_global_fund' },\n        // Overview tab — decomposed PMTCT\n        'overview-pmtct-need': { chartKey: '07a_pmtct_need_arv' },\n        'overview-pmtct-rate': { chartKey: '07b_pmtct_mtct_rate' },\n      };\n      Object.entries(mapping).forEach(([elId, config]) => {\n        const el = document.getElementById(elId);\n        const chartKey = typeof config === 'string' ? config : config.chartKey;\n        const sourceOpt = UNAIDS_CHARTS[chartKey];\n        const opt = sourceOpt ? JSON.parse(JSON.stringify(sourceOpt)) : null;\n        if (opt && typeof config === 'object' && Number.isInteger(config.seriesIndex)) {\n          opt.series = [opt.series[config.seriesIndex]].filter(Boolean);\n          opt.legend = { show: false };\n          if (opt.yAxis && !Array.isArray(opt.yAxis)) opt.yAxis.name = config.yName || opt.yAxis.name || '';\n        }\n        if (!el) { console.warn('Element not found:', elId); return; }\n        if (!opt) { console.warn('Chart config missing:', chartKey); return; }\n",
    )

    match = re.search(r"const UNAIDS_CHARTS = (.*?);;;;;;;;;;;;;;", text, flags=re.S)
    if not match:
        raise RuntimeError(f"Could not find UNAIDS_CHARTS in {path}")
    charts = json.loads(match.group(1))
    prep = charts["11_prep_coverage"]
    prep["xAxis"]["data"] = [label for label, _ in PREP_QUARTERLY]
    prep["yAxis"]["name"] = "New enrollees"
    prep["series"] = [
        {
            "name": "New PrEP enrollees",
            "type": "line",
            "smooth": True,
            "lineStyle": {"width": 3, "color": "#27ae60"},
            "areaStyle": {"opacity": 0.1, "color": "#27ae60"},
            "symbol": "circle",
            "symbolSize": 7,
            "itemStyle": {"color": "#27ae60"},
            "data": [value for _, value in PREP_QUARTERLY],
        }
    ]
    charts["02_cascade_95"]["series"][0]["data"] = [NATIONAL["diagnosed_pct"], NATIONAL["on_art_pct"], NATIONAL["suppressed_pct_of_on_art"]]
    replacement = "const UNAIDS_CHARTS = " + json.dumps(charts, ensure_ascii=False, separators=(",", ":")) + ";;;;;;;;;;;;;;"
    text = text[: match.start()] + replacement + text[match.end() :]
    path.write_text(text, encoding="utf-8")


def write_extraction_json() -> None:
    payload = {
        "source_pdf": str(SOURCE_PDF),
        "source_url": SOURCE_URL,
        "period": PERIOD,
        "national_cascade": NATIONAL,
        "prep_quarterly": PREP_QUARTERLY,
        "diagnosis": DIAGNOSIS,
        "monthly_cases": MONTHLY_CASES,
        "monthly_averages": MONTHLY_AVERAGES,
        "new_cases_by_region": NEW_CASES_REGION,
        "region_cumulative_cases": REGION_CUMULATIVE,
        "mode_of_transmission": MOT,
        "advanced_hiv_disease": AHD,
        "art": ART,
        "treatment_outcome_by_facility_region": TREATMENT_OUTCOME,
        "viral_load_by_facility_region": VL_FACILITY,
        "viral_load_annual": VL_ANNUAL,
        "mortality": MORTALITY,
        "pregnant_women": PREGNANT,
        "transgender_women": TGW,
        "migrant_workers": MIGRANT,
        "transactional_sex": TRANSACTIONAL,
        "transactional_sex_table": TRANSACTIONAL_TABLE,
        "regional_cascade": REGIONAL_CASCADE,
        "age_cascade": AGE_CASCADE,
        "key_population_cascade": KEYPOP_CASCADE,
    }
    save_json(NORMALIZED / "hasp_2026_q1_extracted.json", payload)


def main() -> None:
    SOURCE_DIST.parent.mkdir(parents=True, exist_ok=True)
    if SOURCE_PDF.exists():
        shutil.copy2(SOURCE_PDF, SOURCE_DIST)
    write_extraction_json()
    update_publication_assets()
    update_observations_and_summary()
    update_html(ROOT / "dist" / "index.html")
    update_html(ROOT / "apps_script" / "Index.html")


if __name__ == "__main__":
    main()
