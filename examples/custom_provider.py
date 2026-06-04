"""Example: register your own test-data provider.

Run with:  --provider faker_rows  (after importing this module somewhere on startup)
This is just a template — adapt to your needs.
"""

from save_gcp_local.providers import register, DataProvider


@register
class FixedRowsProvider(DataProvider):
    """Trivial provider: writes a fixed tiny CSV regardless of source.

    Demonstrates the contract. A real custom provider might call an internal
    data API, anonymize fields, join reference tables, etc.
    """

    name = "fixed_rows"

    def materialize(self, source: str, dest: str, **opts) -> str:
        n = int(opts.get("rows", 10))
        with open(dest, "w") as f:
            f.write("id,value\n")
            for i in range(n):
                f.write(f"{i},{i * 10}\n")
        return dest
