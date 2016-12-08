

options(java.parameters = "-Xmx8048m")

suppressMessages(library(RJDBC))
suppressMessages(library(teradataR))
suppressMessages(library(survey))
suppressMessages(library(dplyr))
suppressMessages(library(magrittr))

options(scipen=999)


# CONEXAO TERADATA --------------------------------------------------------


.jinit()
.jaddClassPath('/home/matheusrabetti/Desktop/data-integration/lib/terajdbc4.jar')
.jaddClassPath("/home/matheusrabetti/Desktop/data-integration/lib/tdgssconfig.jar")

teradataR::tdConnect("10.252.1.21/TMODE=ANSI,CHARSET=UTF8",
                     uid = "d_unb_carga",
                     pwd = "d_unb_carga", dType = 'jdbc')



lista_tab = dbListTables(tdConnection)

lista_tab[grepl('pnadc_', lista_tab)]

# DROPAR TUDO
# for(i in lista_tab[grepl('pnadc_', lista_tab)]){
#   print(i)
#   try(tdQuery(sprintf('drop table dev_acc.%s', i)))
# }

## PROBLEMAS COM CAST NAS VARIAVEIS GERADAS PELO R
tdQuery('select * from dev_dw.dim_uf')

tdQuery('select dim.NOUF, pnad.ocup, pnad.ci_l, pnad.ci_u, pnad.erro, pnad.ano, pnad.trimestre 
        from dev_acc.pnadc_estoque_t as pnad
        inner join dev_dw.dim_uf as dim
        on pnad.UF = dim.CDUF')

tdQuery('alter table DEV_ACC.pnadc_estoque_t add CODUF CHAR(2) NULL;')
tdQuery('update DEV_ACC.pnadc_estoque_t
        set NOMEUF = cast(cast(cast(UF as float) as integer) as char(2));')

tdQuery('update pnad
        from dev_acc.pnadc_estoque_t as pnad, dev_dw.dim_uf as dim 
        set NOMEUF = dim.NOUF
        where NOMEUF = dim.CDUF;')

tdQuery('show table dev_acc.pnadc_estoque_t')


# Estoque
tdQuery('select top 20 * from dev_acc.pnadc_estoque_t')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_estoque_t group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_estoque_grst_t group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_estoque_f group by ano, trimestre order by ano,trimestre')

# Taxa participacao
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txpart_grst_t group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txpart_grst_f group by ano, trimestre order by ano, trimestre')
# tdQuery('drop table dev_acc.pnadc_txpart_subst_t')

tdQuery('select top 20 * from dev_acc.pnadc_txpart_grst_t')
tdQuery('select top 20 * from dev_acc.pnadc_txpart_grst_f')
tdQuery('select top 2 * from dev_acc.pnadc_txpart_subst_f')

# Taxa Informalidade
# tdQuery('drop table dev_acc.pnadc_informal')

tdQuery('select top 20 * from dev_acc.pnadc_informal')
tdQuery('select top 20 * from dev_acc.pnadc_informal_grst')
tdQuery('select top 20 * from dev_acc.pnadc_informal_subst')

tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_informal group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_informal_grst group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_informal_subst group by ano, trimestre order by ano, trimestre')

# Desocupados
tdQuery('select top 200 * from dev_acc.pnadc_desocup')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup_cond_dom group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup_fxetar group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup_grst group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup_intr group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_desocup_sexo group by ano, trimestre order by ano, trimestre')

# Tx desemprego
tdQuery('select top 200 * from dev_acc.pnadc_txdesemp')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp_cond_dom group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp_fxetar group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp_grst group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp_intr group by ano, trimestre order by ano, trimestre')
tdQuery('select count(*) as freq, ano, trimestre from dev_acc.pnadc_txdesemp_sexo group by ano, trimestre order by ano, trimestre')



# Delete unique
tdQuery('delete FROM dev_acc.pnadc_informal
where tx_informal = (
SELECT tx_informal FROM dev_acc.pnadc_informal
LEFT OUTER JOIN (
select max(tx_informal) as teste
from dev_acc.pnadc_informal
group by ano, trimestre, uf ) as KeepRows 
ON pnadc_informal.tx_informal = KeepRows.teste
WHERE KeepRows.teste IS NULL)')

