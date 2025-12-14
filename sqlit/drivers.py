"""ODBC Driver detection and installation helpers."""

from __future__ import annotations

import platform
from dataclasses import dataclass


# Supported SQL Server ODBC drivers in order of preference
SUPPORTED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "ODBC Driver 11 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server",
]


@dataclass
class InstallCommand:
    """Installation command for a specific OS."""

    description: str
    commands: list[str]
    requires_sudo: bool = True


def get_installed_drivers() -> list[str]:
    """Get list of installed ODBC drivers for SQL Server."""
    installed = []

    try:
        import pyodbc

        available = [d for d in pyodbc.drivers()]
        for driver in SUPPORTED_DRIVERS:
            if driver in available:
                installed.append(driver)
    except ImportError:
        pass

    return installed


def get_best_driver() -> str | None:
    """Get the best available driver, or None if none installed."""
    installed = get_installed_drivers()
    return installed[0] if installed else None


def get_os_info() -> tuple[str, str]:
    """Get OS type and version."""
    system = platform.system().lower()

    if system == "linux":
        # Try to get distro info
        try:
            with open("/etc/os-release") as f:
                info = {}
                for line in f:
                    if "=" in line:
                        key, value = line.strip().split("=", 1)
                        info[key] = value.strip('"')
                distro = info.get("ID", "unknown")
                version = info.get("VERSION_ID", "")
                return distro, version
        except FileNotFoundError:
            return "linux", ""
    elif system == "darwin":
        return "macos", platform.mac_ver()[0]
    elif system == "windows":
        return "windows", platform.version()

    return system, ""


def get_install_commands(driver: str = "ODBC Driver 18 for SQL Server") -> InstallCommand | None:
    """Get installation commands for the current OS."""
    os_type, os_version = get_os_info()

    if os_type == "macos":
        return InstallCommand(
            description="Install via Homebrew",
            commands=[
                "brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release",
                "brew update",
                f"HOMEBREW_ACCEPT_EULA=Y brew install {'msodbcsql18' if '18' in driver else 'msodbcsql17'}",
            ],
            requires_sudo=False,
        )

    elif os_type == "ubuntu":
        driver_pkg = "msodbcsql18" if "18" in driver else "msodbcsql17"
        version = os_version or "22.04"
        return InstallCommand(
            description="Install on Ubuntu",
            commands=[
                "curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc",
                f"curl https://packages.microsoft.com/config/ubuntu/{version}/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list",
                "sudo apt-get update",
                f"sudo ACCEPT_EULA=Y apt-get install -y {driver_pkg}",
            ],
        )

    elif os_type == "debian":
        driver_pkg = "msodbcsql18" if "18" in driver else "msodbcsql17"
        # Debian version mapping: 11=bullseye, 12=bookworm
        version = os_version.split(".")[0] if os_version else "12"
        return InstallCommand(
            description="Install on Debian",
            commands=[
                "curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc",
                f"curl https://packages.microsoft.com/config/debian/{version}/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list",
                "sudo apt-get update",
                f"sudo ACCEPT_EULA=Y apt-get install -y {driver_pkg}",
            ],
        )

    elif os_type == "fedora":
        driver_pkg = "msodbcsql18" if "18" in driver else "msodbcsql17"
        return InstallCommand(
            description="Install on Fedora",
            commands=[
                "sudo curl https://packages.microsoft.com/config/rhel/9/prod.repo -o /etc/yum.repos.d/mssql-release.repo",
                "sudo dnf remove unixODBC-utf16 unixODBC-utf16-devel",
                f"sudo ACCEPT_EULA=Y dnf install -y {driver_pkg}",
            ],
        )

    elif os_type in ("rhel", "centos", "rocky", "almalinux"):
        driver_pkg = "msodbcsql18" if "18" in driver else "msodbcsql17"
        version = os_version.split(".")[0] if os_version else "9"
        return InstallCommand(
            description=f"Install on {os_type.upper()}",
            commands=[
                f"sudo curl https://packages.microsoft.com/config/rhel/{version}/prod.repo -o /etc/yum.repos.d/mssql-release.repo",
                "sudo yum remove unixODBC-utf16 unixODBC-utf16-devel",
                f"sudo ACCEPT_EULA=Y yum install -y {driver_pkg}",
            ],
        )

    elif os_type == "arch":
        return InstallCommand(
            description="Install on Arch Linux (AUR) - alternatively use: paru -S msodbcsql",
            commands=[
                "yay -S msodbcsql",
            ],
            requires_sudo=False,
        )

    elif os_type == "windows":
        return InstallCommand(
            description="Install via winget (or download from https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)",
            commands=[
                "winget install Microsoft.msodbcsql.18",
            ],
            requires_sudo=False,
        )

    return None


def check_pyodbc_installed() -> bool:
    """Check if pyodbc is installed."""
    try:
        import pyodbc
        return True
    except ImportError:
        return False
