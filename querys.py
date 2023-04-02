


def retorna_associacao():
    call = (("""
                WITH basket as (
                select 
                    MIN(O.foro) AS REG,
                    MIN(i.produtoId) as mdl,
                    COUNT(DISTINCT i.vendaId) as distor,
                    COUNT(DISTINCT i.produtoId) as distit
                    FROM [189.39.29.24].[myboxmarcenaria].[dbo].[VendaItem] AS i
                    inner join [189.39.29.24].[myboxmarcenaria].[dbo].[Venda] AS o
                    ON i.vendaId = o.id
                    GROUP BY o.id
                HAVING
                    COUNT(DISTINCT i.vendaId) = 1 AND 
                    COUNT(i.produtoId) = 1

                )

                select  mdl,COUNT(*) AS cnt
                FROM basket
                group by mdl
                order by cnt DESC"""))


    

def retorna_associacao_cidade():
    call = (("""
    
        WITH md1CTE AS(
            SELECT MIN(o.unidadeId) AS reg,
                MIN(i.produtoId) AS mdl,
                COUNT(DISTINCT i.produtoId) as disstit
                FROM [189.39.29.24].[myboxmarcenaria].[dbo].[VendaItem] AS i
                inner join [189.39.29.24].[myboxmarcenaria].[dbo].[Venda] AS o
                ON i.vendaId = o.id
                group by o.id
                having
            COUNT(DISTINCT i.vendaId) = 1 AND 
                COUNT(i.produtoId) = 1
        )

        select mdl,
            count(disstit) as produtos,reg,COUNT(disstit) AS SUPORT
            FROM md1CTE
        GROUP BY reg,mdl"""))
    
    
    
    
       


def frequencias_leads():
    call = (("""
        with tabela_lead_Counts as (
            SELECT
            DISTINCT
            relacionamentoId
            ,statusId
            ,dataCadastro

            FROM [189.39.29.24].[myboxmarcenaria].[dbo].[RelacionamentoStatusLog])

            select DISTINCT a.ref_lead,b.statusId,b.dataCadastro as datastatuslog

            ,(select COUNT(*) 
            from [189.39.29.24].[myboxmarcenaria].[dbo].[RelacionamentoStatusLog] as logv 
            where  logv.relacionamentoId = a.ref_lead and a.statusId =logv.statusId
            ) as frequencia

            from [BI].[comercial].[classificacao_leads] as a

            left join tabela_lead_Counts as b on b.relacionamentoId = a.ref_lead
            WHERE EXISTS (select distinct X.statusId from [comercial].[classificacao_leads] X) AND b.statusId is not null 
            order by b.dataCadastro desc


            --WHERE EXISTS (SELECT X.relacionamentoId FROM [189.39.29.24].[myboxmarcenaria].[dbo].[RelacionamentoStatusLog] X) AND b.statusId is not null

            --WHERE EXISTS (select X.statusId from [comercial].[classificacao_leads] X) AND b.statusId is not null 


                
                
                """))