amostragem <- function(data){
  cat('Iniciando o processo de amostragem \n')
  # amostra da base completa
  pre_w <-
    svydesign(
      ids = ~ UPA, 
      strata = ~ DOM_PROJECAO,
      weights = ~ PESO_DOM_PESSOAS, 
      data = data,
      nest = TRUE
    )
  # pos estratificacao
  df_pos <- data.frame(DOM_PROJECAO = unique(data$DOM_PROJECAO), 
                       Freq = unique(data$PROJ_POPULACAO))
  # final survey design object
  pnadc.surv <- postStratify(pre_w, ~DOM_PROJECAO, df_pos)
  
  options(survey.lonely.psu = "adjust")
  return(pnadc.surv)
}

estoque <- function(ano, trimestre){
  # TOTAL
  cat('Calculando as métricas de estoque total \n')
  tempo.ini = Sys.time()
  svyby(~ocup,
        ~UF_DESC, 
        design = pnadc.surv,
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_total
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_total %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  tempo.ini = Sys.time()
  svyby(~ocup,
        ~gr_setor+UF_DESC, 
        design = subset(pnadc.surv, gr_setor != 'Mal Definido'),
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_grsetor_total
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_grsetor_total %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  
  tempo.ini = Sys.time()
  svyby(~ocup,
        ~sub_setor+UF_DESC, 
        design = subset(pnadc.surv, sub_setor != 'Atividades mal definidas'),
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_subsetor_total
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_subsetor_total %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_t', df = estoque_total)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_subst_t', df = estoque_subsetor_total)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_grst_t', df = estoque_grsetor_total)
  
  # FORMAL
  cat('Calculando as métricas de estoque formal \n')
  tempo.ini = Sys.time()
  svyby(~formal,
        ~UF_DESC, 
        design = pnadc.surv,
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_formal
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_formal %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  tempo.ini = Sys.time()
  svyby(~formal,
        ~gr_setor+UF_DESC, 
        design = subset(pnadc.surv, gr_setor != 'Mal Definido'),
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_grsetor_formal
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_grsetor_formal %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  
  tempo.ini = Sys.time()
  svyby(~formal,
        ~sub_setor+UF_DESC, 
        design = subset(pnadc.surv, sub_setor != 'Atividades mal definidas'),
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> estoque_subsetor_formal
  tempo = Sys.time() - tempo.ini; tempo
  
  estoque_subsetor_formal %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_f', df = estoque_formal)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_subst_f', df = estoque_subsetor_formal)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_estoque_grst_f', df = estoque_grsetor_formal)
}

tx_informalidade <- function(ano, trimestre){
  cat('Calculando as métricas de taxa de informalidade \n')
  tempo.ini = Sys.time()
  svyby(~informal,
        ~UF_DESC, 
        design = pnadc.surv, 
        svyratio,
        denominator = ~ocup,
        vartype = c('ci','cv'),
        multicore = T) -> informal
  tempo = Sys.time() - tempo.ini; tempo
  
  informal %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_informal = `informal/ocup`)

  cat('GRANDE SETOR \n')
  tempo.ini = Sys.time()
  svyby(~informal,
        ~gr_setor+UF_DESC,
        design = subset(pnadc.surv, gr_setor != 'Mal Definido'),
        svyratio,
        denominator = ~ocup,
        vartype = c('ci','cv'),
        multicore = T) -> informal_grsetor
  tempo = Sys.time() - tempo.ini; tempo

  informal_grsetor %<>%
    mutate(ano = ano,
           trimestre = trimestre) %>%
    rename(erro = cv,
           tx_informal = `informal/ocup`)

  cat('SUBSETOR \n')
  tempo.ini = Sys.time()
  svyby(~informal,
        ~sub_setor+UF_DESC,
        design = subset(pnadc.surv, sub_setor != 'Atividades mal definidas'),
        svyratio,
        denominator = ~ocup,
        vartype = c('ci','cv'),
        multicore = T) -> informal_subsetor
  tempo = Sys.time() - tempo.ini; tempo

  informal_subsetor %<>%
    mutate(ano = ano,
           trimestre = trimestre) %>%
    rename(erro = cv,
           tx_informal = `informal/ocup`)


  tdWriteTable_ps(databasename = 'DEV_ACC',
                  tablename = 'pnadc_informal', df = informal)
  tdWriteTable_ps(databasename = 'DEV_ACC',
                  tablename = 'pnadc_informal_grst', df = informal_grsetor)
  tdWriteTable_ps(databasename = 'DEV_ACC',
                  tablename = 'pnadc_informal_subst', df = informal_subsetor)
}

tx_participacao <- function(ano, trimestre){
  cat('Calculando as métricas de taxa de participação total \n')
  
  estoque_grsetor_total <-
  tdQuery(
    sprintf('select *
          from dev_acc.pnadc_estoque_grst_t 
          where ano = %s AND trimestre = %s', ano, trimestre) )
  
  pnadc %>% 
    group_by(UF_DESC) %>% 
    summarise(estoque_total = sum(ocup * PESO_DOM_COM_EST)) %>% 
    left_join(estoque_grsetor_total, ., by = 'UF_DESC') -> estoque_grsetor_total
  
  
  estoque_subsetor_total <-
    tdQuery(
      sprintf('select *
          from dev_acc.pnadc_estoque_subst_t 
          where ano = %s AND trimestre = %s', ano, trimestre) )
  
  pnadc %>% 
    group_by(UF_DESC) %>% 
    summarise(estoque_total = sum(ocup * PESO_DOM_COM_EST)) %>% 
    left_join(estoque_subsetor_total, ., by = 'UF_DESC') -> estoque_subsetor_total
  
  estoque_grsetor_total %>% 
    filter(erro <= .15) %>% 
    group_by(UF_DESC) %>% 
    mutate(tx_part = round(ocup/estoque_total*100, 2),
           ano = ano,
           trimestre = trimestre) %>% 
    select(gr_setor, UF_DESC, ano, trimestre, tx_part) -> txpart_grsetor_total
  
  estoque_subsetor_total %>% 
    filter(erro <= .15) %>% 
    group_by(UF_DESC) %>% 
    mutate(tx_part = round(ocup/estoque_total*100, 2),
           ano = ano,
           trimestre = trimestre) %>% 
    select(sub_setor, UF_DESC, ano, trimestre, tx_part) -> txpart_subsetor_total
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txpart_grst_t', df = txpart_grsetor_total)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txpart_subst_t', df = txpart_subsetor_total)
  
  
  cat('Calculando as métricas de taxa de participação formal \n')
  
  estoque_grsetor_formal <-
    tdQuery(
      sprintf('select *
          from dev_acc.pnadc_estoque_grst_f
          where ano = %s AND trimestre = %s', ano, trimestre) )
  
  pnadc %>% 
    group_by(UF_DESC) %>% 
    summarise(estoque_formal = sum(formal * PESO_DOM_COM_EST)) %>% 
    left_join(estoque_grsetor_formal, ., by = 'UF_DESC') -> estoque_grsetor_formal
 
  estoque_subsetor_formal <-
    tdQuery(
      sprintf('select *
          from dev_acc.pnadc_estoque_subst_f
          where ano = %s AND trimestre = %s', ano, trimestre) )
  
  pnadc %>% 
    group_by(UF_DESC) %>% 
    summarise(estoque_formal = sum(formal * PESO_DOM_COM_EST)) %>% 
    left_join(estoque_subsetor_formal, ., by = 'UF_DESC') -> estoque_subsetor_formal
  
  
  
  estoque_grsetor_formal %>% 
    filter(erro <= .15) %>% 
    group_by(UF_DESC) %>% 
    mutate(tx_part = round(formal/estoque_formal*100, 2),
           ano = ano,
           trimestre = trimestre) %>% 
    select(gr_setor, UF_DESC, ano, trimestre, tx_part) -> txpart_grsetor_formal
  
  estoque_subsetor_formal %>% 
    filter(erro <= .15) %>% 
    group_by(UF_DESC) %>% 
    mutate(tx_part = round(formal/estoque_formal*100, 2),
           ano = ano,
           trimestre = trimestre) %>% 
    select(sub_setor, UF_DESC, ano, trimestre, tx_part) -> txpart_subsetor_formal
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txpart_grst_f', df = txpart_grsetor_formal)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txpart_subst_f', df = txpart_subsetor_formal)
  
}

desocupados <- function(ano, trimestre){
  # TOTAL
  cat('Calculando as métricas de desocupados \n')
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~UF_DESC, 
        design = pnadc.surv,
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  cat('GRANDE SETOR \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~gr_setor+UF_DESC, 
        design = subset(pnadc.surv, gr_setor != 'Mal Definido'),
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados_grsetor
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados_grsetor %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  cat('SEXO \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~SEXO_DESC+UF_DESC, 
        design = pnadc.surv,
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados_sexo
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados_sexo %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  cat('ESCOLARIDADE \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~NIVEL_INSTRUCAO_DESC+UF_DESC, 
        design = pnadc.surv,
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados_intrucao
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados_intrucao %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  cat('FAIXA ETARIA \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~fx_etaria+UF_DESC, 
        design = pnadc.surv, 
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados_fxetaria
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados_fxetaria %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  cat('CONDIÇÃO DOMICÍLIO \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~chefe+UF_DESC, 
        design = pnadc.surv, 
        svytotal,
        vartype = c('ci','cv'),
        multicore = T) -> desocupados_chefe
  tempo = Sys.time() - tempo.ini; tempo
  
  desocupados_chefe %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv)
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup', df = desocupados)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup_grst', df = desocupados_grsetor)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup_sexo', df = desocupados_sexo)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup_intr', df = desocupados_intrucao)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup_fxetar', df = desocupados_fxetaria)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_desocup_cond_dom', df = desocupados_chefe)
  

}

tx_desemprego <- function(ano, trimestre){
  cat('Calculando as métricas de taxa de desemprego \n')
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~UF_DESC, 
        design = pnadc.surv, 
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
  
  cat('ESCOLARIDADE \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~NIVEL_INSTRUCAO_DESC+UF_DESC, 
        design = pnadc.surv,
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego_intr
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego_intr %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
  
  cat('GRANDE SETOR \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~gr_setor+UF_DESC, 
        design = subset(pnadc.surv, gr_setor != 'Mal Definido'),
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego_grsetor
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego_grsetor %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
  
  cat('SEXO \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~SEXO_DESC+UF_DESC, 
        design = pnadc.surv,
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego_sexo
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego_sexo %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
 
  cat('FAIXA ETARIA \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~fx_etaria+UF_DESC, 
        design = pnadc.surv, 
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego_fxetaria
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego_fxetaria %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
  
  cat('CONDIÇÃO DOMICÍLIO \n')
  
  tempo.ini = Sys.time()
  svyby(~desocup,
        ~chefe+UF_DESC, 
        design = pnadc.surv, 
        svyratio,
        denominator = ~pea,
        vartype = c('ci','cv'),
        multicore = T) -> tx_desemprego_chefe
  tempo = Sys.time() - tempo.ini; tempo
  
  tx_desemprego_chefe %<>% 
    mutate(ano = ano,
           trimestre = trimestre) %>% 
    rename(erro = cv,
           tx_desemprego = `desocup/pea`)
  
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp', df = tx_desemprego)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp_grst', df = tx_desemprego_grsetor)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp_sexo', df = tx_desemprego_sexo)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp_intr', df = tx_desemprego_intr)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp_fxetar', df = tx_desemprego_fxetaria)
  tdWriteTable_ps(databasename = 'DEV_ACC', 
                  tablename = 'pnadc_txdesemp_cond_dom', df = tx_desemprego_chefe)
  
}