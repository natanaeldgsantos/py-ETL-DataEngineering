# Upload de Dados utilizando pandas 


Carga de dados de diferentes tipos de arquivo para uma tabela no banco de dados utilizando o python pandas


**Realizando conexão com o Banco de Dados**

utilizando por exemplo o sqlalchemy.

    from sqlalchemy import create_engine

    string_connection = f'<dbtype>://<user>:<password>@<host>:<port>/<database>'

    # Exemplo
    # mysql://root:root@localhost:3306/world

    # Criando a Conexão com o banco
    engine = create_engine(string_connection)
    connection = engine.connect()


**Verificando o schema dos dados, tipos**

    print( pd.io.sql.get_schema(df) )

por padrão será gerado um schema com tipos de dados baseados no sqlite, banco default.

Se for passado a conexão com banco a ser utilizado o schema será adptado de acordo com a linguagem do banco

    print( pd.io.sql.get_schema(df, name= <nome da tabela>, con = engine ))



## Dividindo o arquivo em Chunks

Quando o arquivo for muito grande pode ser mais performatico realizar o upload em chunks, ou em pedaços, o que vai poupar o uso de memória.

**Quebrando o arquivo em Chunks com pandas**

    df_iter = pd.read_csv('file.csv', iterator=True, chunksize=<number of rows> )

    # Isso não gera um dataframe mas sim um iterator
    print(type(df_iter))


**Iterando pelos elementos**

    # Utilizamos a função next() do python para retornar o próximo elemento no iterator

    next(df_iter)


**Carregando Dados**

A intenção será:
- Criar a tabela, vazia apenas com os cabeçalhos
- Adicionar os dados, chunk a chunk, na tabela criada.

Vamos criar primeiro a tabela

    # Cria uma nova tabela vazia apenas com os nomes
    # Se a tabela já existir 'replace'

    df.head(n=0).to_sql(con = engine, name= <table name> if_exists='replace')


**Carregando dados na tabela em Chunks**

Vamos usar um loop para carregar todas as iterações

    from time import time

    While True:

        t_start = time()

        df= next(df_iter)

        ## Data Preparation
        # Aplica alguma preparação no chunk de dados como padronizar tipos de dados

        df.to_sql(name=<table name>, con=engine, if_exists='append')        

        t_end = time()
        print(' Carga realizada com sucesso em {%.3f} segundos'.format(t_end - t_start))


