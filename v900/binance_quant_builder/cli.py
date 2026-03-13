from __future__ import annotations

import json

import typer

from .config import dump_config, load_config
from .processor import run_pipeline
from .validate import config_summary, validate_config

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command()
def run(config: str = typer.Option(..., "--config", exists=True, readable=True, help="Chemin vers le fichier YAML/TOML/JSON")):
    """Lance le pipeline avec une configuration structurée."""
    cfg = load_config(config)
    result = run_pipeline(cfg)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@app.command("native-run")
def native_run(config: str = typer.Option(..., "--config", exists=True, readable=True, help="Chemin vers le fichier YAML/TOML/JSON")):
    """Alias pratique pour exécuter le mode natif défini dans la config."""
    cfg = load_config(config)
    cfg.run.use_legacy_pipeline = False
    result = run_pipeline(cfg)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@app.command("show-config")
def show_config(config: str = typer.Option(..., "--config", exists=True, readable=True)):
    cfg = load_config(config)
    typer.echo(json.dumps(dump_config(cfg), ensure_ascii=False, indent=2, default=str))


@app.command("validate-config")
def validate_config_cmd(config: str = typer.Option(..., "--config", exists=True, readable=True)):
    cfg = load_config(config)
    errors = validate_config(cfg)
    payload = {"status": "ok" if not errors else "error", "errors": errors, "config": config_summary(cfg)}
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    raise typer.Exit(code=0 if not errors else 1)


if __name__ == "__main__":
    app()
