"""
setup_db.py — Cria o banco SQLite e popula dados mestres (lojas, vendedores, produtos).
Execute uma vez antes de iniciar o generator e o dashboard.
"""
import sqlite3
import random

import os
DB_PATH = os.environ.get("DB_PATH", "blackfriday.db")

LOJAS = [
    (1, "SP Centro",       "Sudeste", "Física"),
    (2, "SP Paulista",     "Sudeste", "Física"),
    (3, "RJ Ipanema",      "Sudeste", "Física"),
    (4, "BH Savassi",      "Sudeste", "Física"),
    (5, "POA Moinhos",     "Sul",     "Física"),
    (6, "CWB Batel",       "Sul",     "Física"),
    (7, "Fortaleza Norte", "Nordeste","Física"),
    (8, "Recife Shopping", "Nordeste","Física"),
    (9, "E-commerce",      "Online",  "Online"),
    (10,"App Mobile",      "Online",  "Online"),
]

CATEGORIAS = [
    ("Eletrônicos",       0.55, 0.72),   # (margem_custo, markup_max)
    ("Eletrodomésticos",  0.58, 0.70),
    ("Moda",              0.35, 0.60),
    ("Beleza",            0.30, 0.65),
    ("Esportes",          0.45, 0.68),
    ("Casa & Decoração",  0.40, 0.65),
    ("Livros",            0.25, 0.55),
    ("Brinquedos",        0.42, 0.65),
]

PRODUTOS_POR_CAT = [
    # Eletrônicos
    ("Smartphone Samsung 128GB", "Eletrônicos", 1200, 2499),
    ("Notebook Dell i5",         "Eletrônicos", 2800, 5299),
    ("Smart TV 50\"",            "Eletrônicos", 1600, 2999),
    ("Fone Bluetooth JBL",       "Eletrônicos",  120,  299),
    ("Câmera Sony A6000",        "Eletrônicos", 3200, 5999),
    ("Tablet Samsung",           "Eletrônicos",  900, 1899),
    ("Smartwatch Xiaomi",        "Eletrônicos",  180,  399),
    # Eletrodomésticos
    ("Geladeira Brastemp 400L",  "Eletrodomésticos", 2200, 3999),
    ("Máquina de Lavar Consul",  "Eletrodomésticos", 1100, 2199),
    ("Ar-Condicionado 12k",      "Eletrodomésticos", 1800, 3299),
    ("Aspirador Electrolux",     "Eletrodomésticos",  350,  799),
    ("Micro-ondas Panasonic",    "Eletrodomésticos",  280,  599),
    # Moda
    ("Tênis Nike Air Max",       "Moda",  180,  449),
    ("Jaqueta Jeans Feminina",   "Moda",   80,  229),
    ("Relógio Casio Clássico",   "Moda",  120,  329),
    ("Bolsa Couro Sintético",    "Moda",   95,  279),
    ("Óculos de Sol Ray-Ban",    "Moda",  250,  649),
    # Beleza
    ("Perfume Boticário 100ml",  "Beleza",  85,  229),
    ("Kit Skincare Neutrogena",  "Beleza",  60,  169),
    ("Secador Mondial 2200W",    "Beleza",  70,  159),
    ("Babyliss Titanium",        "Beleza", 120,  299),
    # Esportes
    ("Bicicleta Speed 21v",      "Esportes", 800, 1799),
    ("Halteres 10kg Par",        "Esportes",  80,  199),
    ("Tapete Yoga Premium",      "Esportes",  45,  119),
    ("Mochila Trilha 45L",       "Esportes", 110,  279),
    # Casa & Decoração
    ("Sofá 3 Lugares Veludo",    "Casa & Decoração", 1200, 2799),
    ("Conjunto Panelas Tramontina","Casa & Decoração", 180,  449),
    ("Luminária LED Inteligente","Casa & Decoração",  90,  219),
    ("Jogo de Cama Queen 300 Fios","Casa & Decoração",140,  329),
    # Livros
    ("Box Harry Potter",         "Livros",  80,  219),
    ("Clean Code - Robert Martin","Livros",  60,  149),
    ("Pai Rico Pai Pobre",        "Livros",  25,   69),
    # Brinquedos
    ("LEGO City Aeroporto",      "Brinquedos", 280,  649),
    ("Boneca Barbie Fashionista", "Brinquedos", 45,  119),
    ("Carrinho Hot Wheels Kit",  "Brinquedos",  30,   89),
    ("Jogo de Tabuleiro Catan",  "Brinquedos", 110,  249),
]

NOMES = [
    "Ana Silva","Bruno Costa","Carla Mendes","Diego Rocha","Elena Faria",
    "Felipe Nunes","Gabriela Lima","Henrique Souza","Isabela Ferreira","João Alves",
    "Karen Moura","Lucas Barbosa","Mariana Pereira","Nicolas Dias","Olivia Cunha",
    "Pedro Matos","Queila Santos","Rafael Teixeira","Sabrina Lopes","Thiago Gomes",
    "Ursula Vieira","Victor Ramos","Wanda Pinto","Xavier Freitas","Yasmin Correia",
    "Zara Nascimento","Adriano Cardoso","Beatriz Martins","Caio Ribeiro","Daniela Cruz",
    "Eduardo Monteiro","Fernanda Araújo","Guilherme Peixoto","Helena Carvalho","Ivan Torres",
    "Juliana Castro","Kevin Medeiros","Laís Fernandes","Marcos Oliveira","Natalia Campos",
    "Orlando Rezende","Patricia Borges","Quintino Leite","Renata Brito","Sergio Andrade",
    "Tatiane Nogueira","Ulisses Macedo","Vanessa Azevedo","Wellington Duarte","Xiomara Lacerda",
]

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Se o banco já tem dados, não recria
    existing = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vendas'").fetchone()
    if existing:
        print(f"Banco '{DB_PATH}' já existe, mantendo dados.")
        conn.close()
        return

    c.executescript("""
        CREATE TABLE IF NOT EXISTS lojas (
            id     INTEGER PRIMARY KEY,
            nome   TEXT NOT NULL,
            regiao TEXT NOT NULL,
            tipo   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS produtos (
            id             INTEGER PRIMARY KEY,
            nome           TEXT NOT NULL,
            categoria      TEXT NOT NULL,
            custo_unitario REAL NOT NULL,
            preco_base     REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS vendedores (
            id       INTEGER PRIMARY KEY,
            nome     TEXT NOT NULL,
            loja_id  INTEGER NOT NULL,
            meta_gmv REAL NOT NULL,
            FOREIGN KEY (loja_id) REFERENCES lojas(id)
        );

        CREATE TABLE IF NOT EXISTS vendas (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id      TEXT NOT NULL,
            timestamp      DATETIME NOT NULL,
            vendedor_id    INTEGER NOT NULL,
            loja_id        INTEGER NOT NULL,
            produto_id     INTEGER NOT NULL,
            canal          TEXT NOT NULL,
            quantidade     INTEGER NOT NULL,
            preco_unitario REAL NOT NULL,
            desconto_pct   REAL NOT NULL,
            valor_liquido  REAL NOT NULL,
            custo_total    REAL NOT NULL,
            status         TEXT NOT NULL,
            FOREIGN KEY (vendedor_id) REFERENCES vendedores(id),
            FOREIGN KEY (loja_id)     REFERENCES lojas(id),
            FOREIGN KEY (produto_id)  REFERENCES produtos(id)
        );

        CREATE INDEX IF NOT EXISTS idx_vendas_ts     ON vendas(timestamp);
        CREATE INDEX IF NOT EXISTS idx_vendas_status ON vendas(status);
    """)

    c.executemany("INSERT INTO lojas VALUES (?,?,?,?)", LOJAS)

    for i, (nome, cat, custo, preco) in enumerate(PRODUTOS_POR_CAT, start=1):
        c.execute("INSERT INTO produtos VALUES (?,?,?,?,?)", (i, nome, cat, custo, preco))

    random.seed(42)
    loja_ids = [l[0] for l in LOJAS]
    for i, nome in enumerate(NOMES, start=1):
        loja_id = random.choice(loja_ids)
        meta = random.randint(80_000, 300_000)
        c.execute("INSERT INTO vendedores VALUES (?,?,?,?)", (i, nome, loja_id, meta))

    conn.commit()
    conn.close()
    print(f"Banco '{DB_PATH}' criado com sucesso.")
    print(f"  {len(LOJAS)} lojas | {len(PRODUTOS_POR_CAT)} produtos | {len(NOMES)} vendedores")

if __name__ == "__main__":
    main()
