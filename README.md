# InsightGrid v2

InsightGrid is a modular data processing backend for enterprise use, built with **FastAPI** and designed for flexibility, dynamic imports, and multi-tool integration.

## ⚠️ Important ⚠️ 

This project was made in spanish.

Some files (such as .html files), file names or console logs will continue to be written in spanish because (right now) all users are native spanish speakers and i'm the only one debugging this.

## 🔍 Overview

> ⚠️ This is **Version 2**. The previous codebase was discarded due to the presence of sensitive and legacy data. This version is clean, modular, and more maintainable.

InsightGrid allows authenticated users to:

- Upload and process Excel/CSV files using custom company-linked tools
- Manage users, companies, and permissions
- Use both single and multi-file processors (e.g., `ventas`, `cruce_ventas`)
- Download processed outputs and review history
- Use PDF guides per tool for documentation
- Integrate seamlessly with SSO and admin panels

## 🛠 Tech Stack

- **FastAPI** – API Framework
- **SQLAlchemy** – ORM
- **Jinja2** – Dashboard templating
- **PostgreSQL** – Database (assumed from DB access patterns)
- **SessionMiddleware** – User session management
- **Dynamic Importing** – For tool processing logic
- **SSO Authentication** – With session-based login

## 📁 Features

- Dynamic tool loading via environment-configured module paths
- Healthcheck endpoint (`/health`) for deployment status
- Admin user auto-creation at startup
- Session-based user access tied to email
- Download & process Excel files with user-based history tracking

🧩 Notes
- All tools must expose either `process_file()` or `process_files()` in the dynamically imported module.
- Tool access is scoped per user/company.
- Admin can view all data; regular users only see assigned companies/tools.
- Procfile and railway.json exist due to Railway deploy
