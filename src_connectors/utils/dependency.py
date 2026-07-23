import re
from pathlib import Path
from typing import List, Dict, Tuple
import structlog

from src_connectors.exceptions.spark import DependencyConflictError

logger = structlog.get_logger(__name__)


def parse_jar_filename(jar_path: str) -> Tuple[str, str]:
    """
    Extracts artifact_id and version from a JAR file path using standard naming conventions.
    Example: '/path/to/ojdbc8-19.3.0.0.jar' -> ('ojdbc8', '19.3.0.0')
    """
    filename = Path(jar_path).stem
    match = re.match(r"^(.+?)-(\d+\.[\w\.\-]+)$", filename)
    if match:
        return match.group(1).lower(), match.group(2)
    return filename.lower(), "unknown"


def resolve_via_ivy_and_check_conflicts(
    maven_packages: List[str],
    local_jar_paths: List[str],
    ivy_cache_dir: str = "/tmp/.ivy2"
) -> Tuple[List[str], List[str]]:
    """
    Inspects, deduplicates, and cross-checks dependencies between Maven Packages and Local JARs.
    """
    jar_registry: Dict[str, Tuple[str, str, str]] = {}
    clean_local_paths = sorted(list(set(j.strip() for j in local_jar_paths if j and j.strip())))

    for path_str in clean_local_paths:
        artifact, version = parse_jar_filename(path_str)
        if artifact in jar_registry:
            exist_ver, exist_path, _ = jar_registry[artifact]
            if exist_ver != version and version != "unknown":
                raise DependencyConflictError(
                    f"Conflict in Local JARs! Artifact '{artifact}' has multiple versions:\n"
                    f"  - Path A: {exist_path} (Version: {exist_ver})\n"
                    f"  - Path B: {path_str} (Version: {version})"
                )
        else:
            jar_registry[artifact] = (version, path_str, "Local JAR")

    clean_packages = sorted(list(set(p.strip() for p in maven_packages if p and p.strip())))
    final_maven_packages = []

    for pkg in clean_packages:
        parts = pkg.split(":")
        if len(parts) >= 3:
            artifact = parts[1].lower()
            version = parts[-1]

            if artifact in jar_registry:
                exist_ver, exist_src, source = jar_registry[artifact]
                if exist_ver != "unknown" and exist_ver != version:
                    raise DependencyConflictError(
                        f"Cross-Source Dependency Conflict Detected!\n"
                        f"Artifact '{artifact}' is declared in BOTH Local JARs and Maven Packages with DIFFERENT versions:\n"
                        f"  - Local JAR: {exist_src} (Version: {exist_ver})\n"
                        f"  - Maven Package: {pkg} (Version: {version})"
                    )
                elif exist_ver == version and source == "Local JAR":
                    logger.info("Skipping Maven package as exact Local JAR already exists", package=pkg, local_jar=exist_src)
                    continue

            jar_registry[artifact] = (version, pkg, "Maven Package")
            final_maven_packages.append(pkg)

    return final_maven_packages, clean_local_paths
