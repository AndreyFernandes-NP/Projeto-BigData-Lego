from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class ProjectPaths:
    root:           Path   # Projeto-Catapulta-DOE/
    data:           Path   # data/
    src:            Path   # src/
    notebooks:      Path   # notebooks/

def get_project_paths(root: Path | None = None) -> ProjectPaths:
    project_root = root or Path(__file__).resolve().parents[1]
    data_dir = project_root / "data"
    src_dir  = project_root / "src"

    return ProjectPaths(
        root           = project_root,
        src            = src_dir,
        data           = data_dir,
        notebooks      = project_root / "notebooks",
    )

# Instância global, só importar em qualquer módulo do projeto
PATHS = get_project_paths()
