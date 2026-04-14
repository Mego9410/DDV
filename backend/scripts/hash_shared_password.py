from __future__ import annotations

import argparse

from passlib.hash import bcrypt


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate bcrypt hash for shared password.")
    ap.add_argument("password", help="The shared password to hash")
    args = ap.parse_args()
    print(bcrypt.hash(args.password))


if __name__ == "__main__":
    main()

