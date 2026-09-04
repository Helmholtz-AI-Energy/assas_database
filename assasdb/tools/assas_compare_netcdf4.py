#!/usr/bin/env python
"""Compare two netCDF4 files variable by variable.

Intended to diff the output of the grouped conversion path
(``assas_single_file_convert.py``) against the flat one
(``assas_single_file_convert_flat.py``). Variables are matched by name,
regardless of whether they sit at root level or inside a (sub)group, since the
two paths place them differently.

For every common variable the shapes are compared and, where they are
compatible, the values over the common leading (time) length. NaN is treated as
equal to NaN, which matters because both paths fill absent data with NaN.

Needs no ASTEC environment, only ``netCDF4`` and ``numpy``.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import netCDF4
import numpy as np

logger = logging.getLogger("assas_app")


def collect_variables(
    group: netCDF4.Dataset, path_prefix: str = ""
) -> Dict[str, Tuple[str, netCDF4.Variable]]:
    """Collect all variables of a netCDF4 file, recursing into groups.

    Args:
        group: The netCDF4 dataset or group to walk.
        path_prefix (str): Path of the current group, empty for the root.

    Returns:
        Dict[str, Tuple[str, netCDF4.Variable]]: Variable name mapped to its
        location path and the variable object. If the same name occurs in
        several groups, the first one found wins and a warning is logged.

    """
    variables: Dict[str, Tuple[str, netCDF4.Variable]] = {}

    for name, variable in group.variables.items():
        location = f"{path_prefix}/{name}" if path_prefix else f"/{name}"
        if name in variables:
            logger.warning(f"Duplicate variable name {name}, keeping first occurrence.")
            continue
        variables[name] = (location, variable)

    for subgroup_name, subgroup in group.groups.items():
        prefix = (
            f"{path_prefix}/{subgroup_name}" if path_prefix else f"/{subgroup_name}"
        )
        for name, entry in collect_variables(subgroup, prefix).items():
            if name in variables:
                logger.warning(
                    f"Variable {name} exists in {variables[name][0]} and {entry[0]}, "
                    "keeping the first one."
                )
                continue
            variables[name] = entry

    return variables


def compare_arrays(
    left: np.ndarray,
    right: np.ndarray,
    rtol: float,
    atol: float,
) -> dict:
    """Compare two arrays element wise, treating NaN as equal to NaN.

    Args:
        left (np.ndarray): Values of the first file.
        right (np.ndarray): Values of the second file.
        rtol (float): Relative tolerance.
        atol (float): Absolute tolerance.

    Returns:
        dict: Number of differing elements, their fraction, the maximum absolute
        difference and the number of positions where only one side is NaN.

    """
    left = np.ma.filled(np.asarray(left, dtype=np.float64), np.nan)
    right = np.ma.filled(np.asarray(right, dtype=np.float64), np.nan)

    left_nan = np.isnan(left)
    right_nan = np.isnan(right)
    nan_mismatch = int(np.count_nonzero(left_nan != right_nan))

    close = np.isclose(left, right, rtol=rtol, atol=atol, equal_nan=True)
    n_different = int(np.count_nonzero(~close))

    both_finite = ~left_nan & ~right_nan
    if np.any(both_finite):
        max_abs_diff = float(np.max(np.abs(left[both_finite] - right[both_finite])))
    else:
        max_abs_diff = 0.0

    return {
        "n_elements": int(left.size),
        "n_different": n_different,
        "fraction_different": n_different / left.size if left.size else 0.0,
        "max_abs_diff": max_abs_diff,
        "nan_mismatch": nan_mismatch,
    }


def compare_files(
    left_path: str,
    right_path: str,
    rtol: float,
    atol: float,
    maximum_index: int = None,
) -> Tuple[List[dict], List[str], List[str]]:
    """Compare all variables of two netCDF4 files.

    Args:
        left_path (str): Path to the first netCDF4 file.
        right_path (str): Path to the second netCDF4 file.
        rtol (float): Relative tolerance for the value comparison.
        atol (float): Absolute tolerance for the value comparison.
        maximum_index (int, optional): Compare only the first N entries along the
            leading dimension. If ``None``, the common length is used.

    Returns:
        Tuple[List[dict], List[str], List[str]]: Per variable results, names only
        present in the first file and names only present in the second file.

    """
    results: List[dict] = []

    with (
        netCDF4.Dataset(left_path, "r") as left_file,
        netCDF4.Dataset(right_path, "r") as right_file,
    ):
        left_variables = collect_variables(left_file)
        right_variables = collect_variables(right_file)

        only_left = sorted(set(left_variables) - set(right_variables))
        only_right = sorted(set(right_variables) - set(left_variables))
        common = sorted(set(left_variables) & set(right_variables))

        logger.info(
            f"{len(left_variables)} variables in {left_path}, "
            f"{len(right_variables)} in {right_path}, {len(common)} in common."
        )

        for name in common:
            left_location, left_variable = left_variables[name]
            right_location, right_variable = right_variables[name]

            result = {
                "name": name,
                "left_location": left_location,
                "right_location": right_location,
                "left_shape": tuple(left_variable.shape),
                "right_shape": tuple(right_variable.shape),
                "status": "equal",
                "compared_length": None,
            }

            if left_variable.shape[1:] != right_variable.shape[1:]:
                result["status"] = "shape mismatch"
                results.append(result)
                logger.warning(
                    f"Variable {name}: trailing shape {left_variable.shape[1:]} "
                    f"vs {right_variable.shape[1:]}, skipping value comparison."
                )
                continue

            if left_variable.ndim == 0:
                length = None
                left_values = left_variable[...]
                right_values = right_variable[...]
            else:
                length = min(left_variable.shape[0], right_variable.shape[0])
                if maximum_index is not None:
                    length = min(length, maximum_index)
                left_values = left_variable[:length, ...]
                right_values = right_variable[:length, ...]

            result["compared_length"] = length

            comparison = compare_arrays(left_values, right_values, rtol=rtol, atol=atol)
            result.update(comparison)

            if comparison["n_different"] > 0:
                result["status"] = "values differ"

            results.append(result)

    return results, only_left, only_right


def report(
    results: List[dict],
    only_left: List[str],
    only_right: List[str],
    left_path: str,
    right_path: str,
    limit: int = 40,
) -> bool:
    """Print a summary of the comparison.

    Args:
        results (List[dict]): Per variable results.
        only_left (List[str]): Variables only present in the first file.
        only_right (List[str]): Variables only present in the second file.
        left_path (str): Path of the first file, used for the header.
        right_path (str): Path of the second file, used for the header.
        limit (int): Maximum number of differing variables to list.

    Returns:
        bool: True if the files are considered identical.

    """
    differing = [result for result in results if result["status"] != "equal"]

    print(f"\nA: {left_path}")
    print(f"B: {right_path}\n")

    print(f"common variables : {len(results)}")
    print(f"only in A        : {len(only_left)}")
    print(f"only in B        : {len(only_right)}")
    print(f"differing        : {len(differing)}\n")

    if only_left:
        print("Only in A:")
        for name in only_left[:limit]:
            print(f"  {name}")
        if len(only_left) > limit:
            print(f"  ... {len(only_left) - limit} more")
        print()

    if only_right:
        print("Only in B:")
        for name in only_right[:limit]:
            print(f"  {name}")
        if len(only_right) > limit:
            print(f"  ... {len(only_right) - limit} more")
        print()

    if differing:
        print(
            f"{'variable':<40} {'status':<15} {'n_diff':>10} {'frac':>8} "
            f"{'max_abs_diff':>14} {'nan_mism':>9}"
        )
        for result in differing[:limit]:
            if result["status"] == "shape mismatch":
                print(
                    f"{result['name']:<40} {result['status']:<15} "
                    f"{str(result['left_shape']):>10} vs {result['right_shape']}"
                )
            else:
                print(
                    f"{result['name']:<40} {result['status']:<15} "
                    f"{result['n_different']:>10} "
                    f"{result['fraction_different']:>8.3f} "
                    f"{result['max_abs_diff']:>14.6g} "
                    f"{result['nan_mismatch']:>9}"
                )
        if len(differing) > limit:
            print(f"... {len(differing) - limit} more")
        print()

    identical = not differing and not only_left and not only_right
    print("Files are identical." if identical else "Files differ.")

    return identical


def main() -> None:
    """Parse command line arguments and compare two netCDF4 files."""
    parser = argparse.ArgumentParser(
        description="Compare two netCDF4 files variable by variable."
    )
    parser.add_argument("left", help="Path to the first .nc file (A)")
    parser.add_argument("right", help="Path to the second .nc file (B)")
    parser.add_argument(
        "--rtol",
        type=float,
        default=1e-5,
        help="Relative tolerance (default: %(default)s)",
    )
    parser.add_argument(
        "--atol",
        type=float,
        default=1e-8,
        help="Absolute tolerance (default: %(default)s)",
    )
    parser.add_argument(
        "-t",
        "--time",
        "--maximum-index",
        dest="maximum_index",
        type=int,
        default=None,
        help="Compare only the first N time points (default: common length)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=40,
        help="Maximum number of variables to list per section (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: %(default)s)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    for path in (args.left, args.right):
        if not Path(path).exists():
            parser.error(f"File does not exist: {path}")

    results, only_left, only_right = compare_files(
        left_path=args.left,
        right_path=args.right,
        rtol=args.rtol,
        atol=args.atol,
        maximum_index=args.maximum_index,
    )

    identical = report(
        results=results,
        only_left=only_left,
        only_right=only_right,
        left_path=args.left,
        right_path=args.right,
        limit=args.limit,
    )

    sys.exit(0 if identical else 1)


if __name__ == "__main__":
    main()
