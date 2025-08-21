# Magellium HRWSI Harvester

Ce projet implémente un collecteur de données (harvester) pour une interface de service web à haute résolution (HRWSI). Il est conçu pour fonctionner selon deux modes distincts : `ARCHIVE` pour le traitement de données historiques sur une période définie, et `NEAR_REAL_TIME` (NRT) pour une collecte de données continue.

L'application est construite sur une architecture logicielle propre et modulaire (Architecture Hexagonale) pour garantir la maintenabilité et l'évolutivité.

## Table des matières
- Fonctionnalités
- Architecture
- Prérequis
- Installation
- Configuration
- Utilisation
- Débogage avec VS Code
- Structure du projet

## Fonctionnalités

- **Double mode d'opération** : `ARCHIVE` et `NEAR_REAL_TIME`.
- **Architecture Hexagonale** : Séparation claire des préoccupations entre le cœur de métier et l'infrastructure.
- **Gestion des dépendances avec Poetry** : Pour un environnement de développement reproductible.
- **Gestion sécurisée des secrets** : Intégration avec HashiCorp Vault.
- **Journalisation (Logging) robuste** : Sortie configurable sur la console et dans des fichiers rotatifs.
- **Intégration de base de données** : Utilise PostgreSQL pour la persistance des données.
- **Planificateur de tâches** : Un `Scheduler` intégré pour exécuter des tâches à intervalles réguliers.

## Architecture

Le projet suit les principes de l'**Architecture Hexagonale** (ou Ports et Adaptateurs). Cette approche isole la logique métier principale des services externes tels que les bases de données, les API ou les interfaces utilisateur.

- **`system/common`**: Contient les utilitaires partagés par toute l'application, comme le `LoggerFactory` et les énumérations (`RunMode`).
- **`system/harvesters`**: Définit le cœur de la logique de collecte. Il contient les services applicatifs (`HarvesterService`) et les ports (interfaces) qui décrivent les contrats avec l'extérieur.
- **`system/launchers`**: Contient la logique de démarrage de l'application. Les `factories` sélectionnent et construisent le service approprié en fonction du mode d'exécution.
- **`serviceproviders`**: Contient les adaptateurs pour les services d'infrastructure externes. Par exemple, `vault.py` est un adaptateur pour le client HashiCorp Vault.

Cette structure permet de remplacer facilement une implémentation d'infrastructure (par exemple, changer de base de données) sans impacter le code métier.

## Prérequis

Avant de commencer, assurez-vous d'avoir installé les éléments suivants :
- Python 3.13+ (basé sur `poetry.lock`)
- Poetry pour la gestion des dépendances.
- Un accès à une instance **PostgreSQL** en cours d'exécution.
- Un accès à une instance **HashiCorp Vault** en cours d'exécution.

## Installation

1.  Clonez le dépôt :
    ```bash
    git clone <your-repository-url>
    cd magellium-hrwsi
    ```

2.  Installez les dépendances du projet avec Poetry :
    ```bash
    poetry install
    ```

## Configuration

L'application est configurée à l'aide de variables d'environnement. Pour le développement local, il est recommandé de créer un fichier `.env` à la racine du projet.

Voici la liste des variables d'environnement requises, basées sur la configuration de lancement de VS Code :

| Variable | Description | Exemple |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------- |
| `HRWSI_ENVIRONMENT_NAME` | L'environnement de déploiement (ex: `dev`, `staging`, `prod`). | `dev` |
| `HRWSI_HARVESTER_RUN_MODE` | Le mode d'opération. Peut être `ARCHIVE` ou `NEAR_REAL_TIME`. | `ARCHIVE` |
| `HRWSI_HARVESTER_DATABASE_HOST` | L'hôte de la base de données PostgreSQL. | `localhost` |
| `HRWSI_HARVESTER_DATABASE_PORT` | Le port de la base de données PostgreSQL. | `5432` |
| `HRWSI_HARVESTER_DATABASE_USER` | Le nom d'utilisateur pour la connexion à la base de données. | `postgres` |
| `HRWSI_HARVESTER_DATABASE_PASSWORD` | Le mot de passe pour la connexion à la base de données. | `postgres` |
| `HRWSI_HARVESTER_DATABASE_NAME` | Le nom de la base de données à utiliser. | `postgres` |
| `HRWSI_HARVESTER_ARCHIVE_START_DATE` | **(Mode `ARCHIVE` uniquement)** Date de début au format `YYYY-MM-DD`. | `2023-01-01` |
| `HRWSI_HARVESTER_ARCHIVE_END_DATE` | **(Mode `ARCHIVE` uniquement)** Date de fin au format `YYYY-MM-DD`. | `2023-01-31` |
| `PYTHONPATH` | Doit être configuré pour inclure le répertoire `src`. | `${workspaceFolder}/src` |


## Utilisation

Pour lancer l'application, vous avez besoin d'un script de point d'entrée (par exemple, `src/main.py`). Ce script est responsable de l'initialisation des services et du lancement du harvester.

Voici un exemple de ce à quoi pourrait ressembler `src/main.py` :

```python
# src/main.py
import os
from magellium.hrwsi.system.common.modes import RunMode
from magellium.hrwsi.system.common.logger import LoggerFactory
from magellium.hrwsi.system.launchers.application.business.services.launcher_factory import LauncherServiceFactory
# NOTE: Vous devrez créer votre propre implémentation du Repository.
# from magellium.hrwsi.adapters.database_repository import DatabaseRepository

LOGGER = LoggerFactory.get_logger(__name__)

def main():
    """
    Point d'entrée principal de l'application.
    """
    try:
        LOGGER.info("Démarrage de l'application HRWSI Harvester...")

        # 1. Charger la configuration depuis les variables d'environnement
        run_mode_str = os.getenv("HRWSI_HARVESTER_RUN_MODE", "ARCHIVE")
        run_mode = RunMode[run_mode_str]
        LOGGER.info(f"Mode d'exécution : {run_mode.name}")

        # 2. Initialiser les adaptateurs (ex: le repository)
        # repository = DatabaseRepository() # Exemple, à adapter à votre implémentation
        repository = None # Placeholder

        # 3. Utiliser la factory pour créer le service de lancement approprié
        launcher_service = LauncherServiceFactory.create_launcher_service(run_mode, repository)

        # 4. Lancer le processus
        launcher_service.launch()

        LOGGER.info("L'application HRWSI Harvester a terminé son exécution.")

    except Exception as e:
        LOGGER.error(f"Une erreur non gérée a interrompu l'application : {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()
```

Une fois le point d'entrée créé, vous pouvez lancer l'application avec Poetry :

```bash
# Assurez-vous que vos variables d'environnement sont chargées
export HRWSI_HARVESTER_RUN_MODE=ARCHIVE
# ... autres variables

poetry run python src/main.py
```

## Débogage avec VS Code

Le projet inclut une configuration de lancement (`.vscode/launch.json`) pour faciliter le débogage dans Visual Studio Code.

1.  Assurez-vous que les variables d'environnement sont définies dans le fichier `launch.json` ou dans un fichier `.env` (si vous utilisez une extension comme "DotENV").
2.  Ouvrez le fichier que vous souhaitez exécuter (par exemple, `src/main.py`).
3.  Allez dans le panneau "Exécuter et déboguer" (Ctrl+Shift+D).
4.  Sélectionnez **"Python Debugger: Current File"** dans le menu déroulant.
5.  Appuyez sur **F5** pour démarrer la session de débogage.

## Structure du projet

```
magellium-hrwsi/
├── .gitignore
├── .vscode/
│   └── launch.json      # Configuration de débogage pour VS Code
├── logs/                # Répertoire de sortie pour les fichiers de log
├── poetry.lock          # Fichier de lock des dépendances Poetry
├── pyproject.toml       # Définition du projet et des dépendances (Poetry)
├── README.md            # Ce fichier
└── src/
    ├── magellium/
    │   ├── hrwsi/
    │   │   ├── system/
    │   │   │   ├── common/        # Utilitaires communs (logger, enums)
    │   │   │   ├── harvesters/    # Logique métier et services de collecte
    │   │   │   └── launchers/     # Logique de démarrage et factories
    │   │   └── serviceproviders/  # Adaptateurs pour services externes (ex: Vault)
    │   └── scheduler.py         # Planificateur de tâches générique
    └── main.py                # Point d'entrée de l'application (à créer)
```