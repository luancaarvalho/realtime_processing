# Desafio — Demo 05 (Event-time vs Processing-time)

## O que fazer

- Usar o `demo5_consumer_exercise.py` para gerar um grafico de dispersao.
- Eixo X: `processing_time`
- Eixo Y: `event_time`
- Cores:
  - verde = on-time
  - laranja = late mas dentro da watermark
  - vermelho = DLQ
- Adicionar a linha de referencia `y = x`.

## Entregavel

- Arquivo do grafico gerado (PNG ou SVG).

## Data de entrega

- 04/02/2026

## Notas sobre o resultado

O gráfico relaciona o tempo de processamento ao tempo de ocorrência dos eventos, permitindo visualizar atrasos, aplicação de watermark e descarte de eventos fora de tolerância.
Eventos classificados como late (laranja) e DLQ (vermelho) podem aparecer próximos no espaço do gráfico. Isso ocorre porque o descarte depende do valor da watermark corrente no exato milissegundo do processamento. Se a watermark já avançou devido a um evento mais novo, eventos antigos são descartados, mesmo que o atraso seja idêntico a um evento aceito anteriormente. O atraso absoluto é a distância vertical até a linha azul, mas o descarte é ditado pelo progresso do fluxo (o maior timestamp visto até então menos a tolerância de 5 minutos).

No terminal para rodar producer e consumer:
```
python aula01/demo05/demo5_producer.py --records 500`
python aula01/demo05/demo5_consumer_exercise.py --records 500 --group eixo_abs --out grafico_eixo_original.png
python aula01/demo05/demo5_consumer_exercise.py --records 500 --group eixo_rel --relative-time --out grafico_eixo_relativo.png
```

### Sobre os parâmetros novos

* `--out` define o nome e o caminho do arquivo de saída do gráfico gerado;
* `--dpi` permite ajustar a resolução da imagem exportada;
* `--relative-time` permite a visualização alternativa dos tempos em escala relativa, tornando o gráfico mais legível sem alterar a lógica de processamento dos eventos.

