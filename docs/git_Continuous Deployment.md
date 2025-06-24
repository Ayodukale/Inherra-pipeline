
### README for Continuous Deployment (`cd.yml`)

You can save this as `.github/workflows/README.md` to keep your documentation organized.

# README: dbt Docs Deployment Workflow (`cd.yml`)

## 🎯 Purpose

This workflow automates the process of publishing your dbt project's documentation to a live, public website, hosted for free on **GitHub Pages**.

**The "Why":**
Instead of your documentation only existing locally on your machine after running `dbt docs serve`, this gives your entire team a permanent, up-to-date, and shareable URL to view the project's data dictionary, model lineage, and test results. It turns your documentation from a personal tool into a team asset.

## ⚙️ How It Works

This workflow is triggered automatically **every time a commit is pushed to the `main` branch**.

When triggered, a robot assistant (the GitHub Actions runner) performs these key steps:

1.  **Sets Up a Clean Environment:** It checks out your code, installs Python, and installs all the necessary packages (`dbt-snowflake`, etc.).
2.  **Securely Configures dbt:** It builds a temporary `profiles.yml` file using the secrets you stored in your GitHub repository settings. This allows it to connect to Snowflake without ever exposing your private key in the code.
3.  **Runs `dbt docs generate`:** This is the core command. It runs your dbt project and compiles the documentation into a set of static HTML, CSS, and JavaScript files inside the `target/` directory.
4.  **Deploys the `target/` Directory:** It takes the entire `target/` folder (which is now a self-contained website) and pushes it to a special branch on GitHub (`gh-pages`) that is specifically for hosting your live site.

## 🫵 What You Need to Do

Your job is simple: **do your work and merge your pull requests into `main`**.

That's it. The rest is automatic.

After you merge a pull request, this workflow will run. Within a minute or two, your live documentation website will be updated with the latest changes from your `main` branch.

## 🌐 Finding Your Live Documentation

The permanent URL for your documentation site is based on your GitHub username and repository name.

Your live dbt documentation is now hosted at:

**[https://ayodukale.github.io/dbt](https://ayodukale.github.io/dbt)**

*(Note: After the very first successful deployment, it can sometimes take 5-10 minutes for the site to become live. Subsequent updates are usually faster.)*

## 🔧 Maintenance & Troubleshooting

The most common reason for this workflow to fail in the future is due to expired or incorrect GitHub Secrets. If this workflow starts failing, the first place to check is:

**GitHub Repository > Settings > Secrets and variables > Actions**

Ensure that all the required secrets (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, etc.) are present and have the correct values.

---

