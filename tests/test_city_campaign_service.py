import json

from src.services.city_campaign_service import (
    build_campaign_jobs,
    parse_cities_markdown,
    resolve_city_bounds,
)


def test_parse_cities_markdown_extracts_ordered_city_names(tmp_path):
    source = tmp_path / "cities.md"
    source.write_text(
        "\n".join(
            [
                "Rang\tStadt\tReddit-URL (Subreddit)",
                "1\tBerlin\tr/berlin",
                "2\tHamburg\tr/hamburg",
                "5\tFrankfurt am Main\tr/frankfurt",
            ]
        ),
        encoding="utf-8",
    )

    cities = parse_cities_markdown(str(source))

    assert cities == ["Berlin", "Hamburg", "Frankfurt am Main"]


def test_build_campaign_jobs_applies_template_and_smoke_slice():
    jobs = build_campaign_jobs(
        ["Berlin", "Hamburg", "Bonn"],
        ["Hotels", "Restaurants", "Cafes"],
        "{category} in {city}",
        smoke_test=True,
        smoke_cities=2,
        smoke_categories=1,
    )

    assert [job.search_term for job in jobs] == [
        "Hotels in Berlin",
        "Hotels in Hamburg",
    ]


def test_resolve_city_bounds_uses_cache_file(tmp_path):
    cache_path = tmp_path / "city_bounds.json"
    cache_path.write_text(
        json.dumps(
            {
                "Berlin": {
                    "bounds": {
                        "min_lat": 52.0,
                        "min_lng": 13.0,
                        "max_lat": 53.0,
                        "max_lng": 14.0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    resolved = resolve_city_bounds(["Berlin"], cache_path=str(cache_path))

    assert resolved == {"Berlin": (52.0, 13.0, 53.0, 14.0)}


def test_build_campaign_jobs_attaches_city_bounds():
    jobs = build_campaign_jobs(
        ["Berlin"],
        ["Hotels"],
        "{category} in {city}",
        bounds_by_city={"Berlin": (52.0, 13.0, 53.0, 14.0)},
    )

    assert jobs[0].bounds == (52.0, 13.0, 53.0, 14.0)
