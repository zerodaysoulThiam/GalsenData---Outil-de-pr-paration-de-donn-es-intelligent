<div align="center">
  <img src="https://res.cloudinary.com/anuraghazra/image/upload/v1594908242/logo_ccswme.svg" width="100px" alt="GitHub Readme Stats" />
  <h1 style="font-size: 28px; margin: 10px 0;">GitHub Readme Stats</h1>
  <p>Get dynamically generated GitHub stats on your READMEs!</p>
</div>

<p align="center">
  <a href="https://github.com/anuraghazra/github-readme-stats/actions">
    <img alt="Tests Passing" src="https://github.com/anuraghazra/github-readme-stats/workflows/Test/badge.svg" />
  </a>
  <a href="https://github.com/anuraghazra/github-readme-stats/graphs/contributors">
    <img alt="GitHub Contributors" src="https://img.shields.io/github/contributors/anuraghazra/github-readme-stats" />
  </a>
  <a href="https://codecov.io/gh/anuraghazra/github-readme-stats">
    <img alt="Tests Coverage" src="https://codecov.io/gh/anuraghazra/github-readme-stats/branch/master/graph/badge.svg" />
  </a>
  <a href="https://github.com/anuraghazra/github-readme-stats/issues">
    <img alt="Issues" src="https://img.shields.io/github/issues/anuraghazra/github-readme-stats?color=0088ff" />
  </a>
  <a href="https://github.com/anuraghazra/github-readme-stats/pulls">
    <img alt="GitHub pull requests" src="https://img.shields.io/github/issues-pr/anuraghazra/github-readme-stats?color=0088ff" />
  </a>
  <a href="https://securityscorecards.dev/viewer/?uri=github.com/anuraghazra/github-readme-stats">
    <img alt="OpenSSF Scorecard" src="https://api.securityscorecards.dev/projects/github.com/anuraghazra/github-readme-stats/badge" />
  </a>
  <br />
  <br />
  <a href="https://vercel.com?utm\_source=github\_readme\_stats\_team\&utm\_campaign=oss">
    <img src="./powered-by-vercel.svg"/>
  </a>
</p>
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
# DataClean — Flask

Application de nettoyage intelligent de données CSV avec authentification JWT.
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
## Installation rapide

```bash
# 1. Cloner ou dézipper le projet
cd securafrik_flask

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# 2. Créer un environnement virtuel
python -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer l'environnement
cp .env.example .env
# Modifier .env si besoin (SQLite par défaut, aucune config requise)

# 5. Lancer
python app.py
```

Ouvrir http://localhost:5000

## Compte de test (créé automatiquement)

Inscrivez-vous via la page de connexion. Les tables sont créées automatiquement au démarrage.

## Configuration base de données

- **SQLite** (défaut) : rien à faire, le fichier `instance/dataclean.db` est créé automatiquement.
- **MySQL** : mettre `DB_ENGINE=mysql` dans `.env` et remplir les variables `DB_*`.

## Architecture

| Fichier | Rôle |
|---|---|
| `app.py` | Routes Flask, logique auth, API REST |
| `database.py` | Modèles SQLAlchemy (User, Session, Job, AuthSession) |
| `processor.py` | Moteur de nettoyage Pandas |
| `templates/login.html` | Page connexion / inscription |
| `templates/index.html` | Application principale |

## API

| Méthode | Route | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/register` | ✗ | Inscription |
| POST | `/api/auth/login` | ✗ | Connexion → token JWT |
| POST | `/api/auth/logout` | ✓ | Déconnexion |
| GET | `/api/auth/me` | ✓ | Utilisateur courant |
| POST | `/api/upload` | ✓ | Upload CSV |
| POST | `/api/process` | ✓ | Lancer le nettoyage |
| GET | `/api/download/<job_id>` | ✓ | Télécharger le CSV nettoyé |
| GET | `/api/history` | ✓ | Historique des sessions |
| GET | `/api/stats` | ✓ | Statistiques |

![Status](https://img.shields.io/badge/Project-Completed-success?style=for-the-badge)
![Level](https://img.shields.io/badge/Level-Intermediate%20%2F%20Advanced-black?style=for-the-badge)

