# olist-ml-models

Projeto de Machine Learning do início ao fim no contexto de um e-commerce.

Os objetivos desse projeto de Ciência de Dados são: 
- Criar um modelo de Machine Learning para ajudar o negócio da empresa Olist, mais especificamente, tentar prever o _Churn_ dos vendedores (que são os clientes da Olist)
- Entender melhor do ambiente do Databricks e todas as suas ferramentas integradas durante o desenvolvimento do projeto.

Este projeto foi desenvolvido no curso de [Machine Learning do Início ao Fim](https://www.twitch.tv/collections/sG1UU3C2UheIPg), resultado de uma parceria entre o canal [Téo Me Why](https://www.twitch.tv/teomewhy) e o [Instituto Aaron Swartz](https://institutoasw.org/).

## Problema de Negócio
A Olist é uma empresa que funciona como intermediário entre vendedores que desejam vender seus produtos online e _marketplaces_, cobrando uma taxa por essa intermediação. Assim, a maioria dos clientes da empresa são pequenos empreendedores e empresas que desejam expor seus produtos digitalmente. 

O LTV (_Lifetime Value_) dos clientes da empresa está caindo, e o time de dados da empresa foi acionado para pensar em alguma solução que ajude a evitar uma queda maior ou que até mesmo aumente essa métrica.

Levantadas algumas ideias, a escolhida foi a de desenvolver um modelo de Machine Learning para prever quais clientes entrarão em _churn_, com o objetivo de promover benefícios ou descontos para esses clientes antes de entrarem em _churn_, mantendo eles ativos na base.

## Metodologia

Todo o desenvolvimento do projeto foi realizado no Databricks, onde as pessoas `assinantes` do canal do [Téo Me Why](https://www.twitch.tv/teomewhy) têm acesso ao Datalake para realizar seus próprios experimentos.

Passei por todas etapas do ciclo analítico, desde ETL das fontes de dados, criação de `feature store`, criação da `ABT` (_Analytical Base Table_), treinamento dos algoritmos, e implementação do algoritmo com melhores resultados para novas predições.

Com o problema bem definido, foi feito um _brainstorm_ para definir quais variáveis poderiam ajudar a prever o evento de interesse (_churn_), com a criação das _queries_ e respectivas tabelas com base no conjunto de dados disponível para a extração dessas variáveis.

Com todas as variáveis criadas e disponíveis, foi criada a tabela definitiva para treinamento de uma algoritmo de Machine Learning. O nome desta tabela é `ABT - *Analytical Base Table*`, onde possui todas informações necessária para solução do problema de negócios, i.e. features (variáveis, características, etc.) e target (variáveis resposta, alvo).

Depois foi a hora de treinar os algoritmos de Machine Learning, utilizando a biblioteca MLFlow para realizar a gestão do ciclo de vida dos modelos. Desta forma, fica muito mais fácil identificar a performance, métricas, parâmetros e variáveis de cada modelo, facilitando assim a tomada de decisão do melhor modelo.

Definido o melhor modelo, torna-se possível realizar novas predições e criar um novo script para fazer este processo de forma automática. Isto é, colocar o modelo em produção e contribuir para a melhora do negócio da empresa.

## Conclusões
Feita a Classificação dos clientes com relação à probabilidade de entrarem em _churn_, a visualização da lista ordenada através de uma tabela no Datalake permite que o time responsável pela relação com o cliente se organize e seja muito mais assertivo em evitar que os clientes entrem em _churn_.

Ainda, o desenvolvimento desse projeto me propiciou um melhor conhecimento do ambiente do Databricks e algumas de suas funcionalidades, como por exemplo, exploração do Data Catalog, criação de _schemas_ e tabelas, integração com o Git/Github e MLFlow, entre outras.
