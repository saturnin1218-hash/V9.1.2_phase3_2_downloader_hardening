from pathlib import Path

from binance_quant_builder.config import load_config


def test_load_toml(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text("""[run]
symbol = 'BTCUSDT'
out_dir = './out'
""", encoding='utf-8')
    cfg = load_config(path)
    assert cfg.run.symbol == 'BTCUSDT'
