# Databricks notebook source
# DBTITLE 1,Instalar Librerías
dbutils.library.installPyPI('tsfresh')
dbutils.library.installPyPI('lightgbm')
dbutils.library.installPyPI('shap')

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from tsfresh import extract_features

# COMMAND ----------

# DBTITLE 1,Contar Registros por mes en base R04
tmp = spark.sql("select periodo_id, count(1) AS N from risgcred_db.lnd_deuda_cmf_tb GROUP BY periodo_id ORDER BY periodo_id")
print((tmp.count(), len(tmp.columns)))
display(tmp)

# COMMAND ----------

# DBTITLE 1,Definir Muestra
dfSample = spark.sql("""select distinct rut, Periodo_Id from risgcred_db.lnd_deuda_cmf_tb tablesample(1 percent) 
                        where Deuda_DVigente  +  Deuda_DMora  +  Deuda_DVencida + Deuda_DCastigo + Disp_Lineas> 0
                        and Periodo_id between '2018-01-01' and  '2018-12-01' and rut < 50000000    """)
print((dfSample.count(), len(dfSample.columns)))
dfSample.createOrReplaceTempView("dfSample")

# COMMAND ----------

# spark.sql("""REFRESH TABLE risgcred_db.lnd_deuda_cmf_tb""")

# COMMAND ----------

# DBTITLE 1,Extraer información histórica R04 para la muestra
dfR04_hist = spark.sql("""select A.Rut, A.Periodo_Id, B.Periodo_Id as P, CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) AS ID,
                      Deuda_DVigente, Deuda_Mex, Deuda_DMora, Deuda_DVencida, Deuda_DCastigo, Deuda_IVigente, Deuda_IVencida, Deuda_ICastigo, 
                      Deuda_DComercial, Deuda_DConsumo, Deuda_DHipotecaria, N_Acreedores, Disp_Lineas, Deuda_DFinInversiones, Deuda_DOpePacto,
                      Deuda_Leasing, Deuda_LeasingMora
                      from dfSample AS A
                        LEFT JOIN
                        risgcred_db.lnd_deuda_cmf_tb AS B
                        ON A.rut = B.Rut 
                        WHERE
                             CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) >= 0
                        and  CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) <= 11   """)
print((dfR04_hist.count(), len(dfR04_hist.columns)))
dfR04_hist_pd = dfR04_hist.select("*").toPandas()
print(dfR04_hist_pd.shape)
# dfR04_hist_pd.sort_values(['Rut','Periodo_Id','P'], ascending = [True,True,False]).head(5)

# COMMAND ----------

# DBTITLE 1,Definir ID_Global
dfR04_hist_pd['ID_GLOBAL'] = dfR04_hist_pd['Rut'].astype(str) + ";" + dfR04_hist_pd['Periodo_Id'].astype(str)
dfR04_hist_pd.drop(['Rut','Periodo_Id','P'], axis = 1, inplace = True)
dfR04_hist_pd.head(3)

# COMMAND ----------

# DBTITLE 1,Transformar variables a float
for c in dfR04_hist_pd.columns:
  if c != 'ID_GLOBAL' and c != 'ID':
    dfR04_hist_pd[c] = dfR04_hist_pd[c].astype(float)

# COMMAND ----------

    fc_parameters = {
     "abs_energy": None,
#     "absolute_sum_of_changes": None,
#     "count_above_mean": None,
#     "count_below_mean": None,
     "kurtosis": None,
    #"longest_strike_above_mean": None,
    #"longest_strike_below_mean": None,
    "maximum": None,
    "mean": None,
    "mean_abs_change": None,
    #"mean_change": None,
    #"mean_second_derivative_central": None,
#     "median": None,
#     "minimum": None,
     "sample_entropy": None,
      "skewness": None,
     "standard_deviation": None,
#     #"sum_values": None,
#     "variance": None,
    } 

# COMMAND ----------

# tmp = dfR04_hist_pd[['ID_GLOBAL']].drop_duplicates().sample(n = 10000)
# tmp = tmp.merge(dfR04_hist_pd, how = 'inner', on = 'ID_GLOBAL')

# COMMAND ----------

dfR04_hist_pd.columns

# COMMAND ----------

tmp = dfR04_hist_pd.loc[:,['ID_GLOBAL', 'ID', 'Deuda_DVigente', 'Deuda_Mex']]
tmp = spark.createDataFrame(tmp)
# tmp['kind'] = str(variable)

df_melted  = pd.melt(tmp, id_vars=['ID_GLOBAL', 'ID'], value_vars=['Deuda_DVigente', 'Deuda_Mex'], var_name="kind", value_name="value")
df_grouped = df_melted.groupby(["ID_GLOBAL", "kind"])


from tsfresh.convenience.bindings import spark_feature_extraction_on_chunk
from tsfresh.feature_extraction import ComprehensiveFCParameters
from tsfresh.feature_extraction.settings import MinimalFCParameters

features = spark_feature_extraction_on_chunk(df_grouped, column_id="ID_GLOBAL",column_kind="kind",column_sort="ID",column_value="value",default_fc_parameters=MinimalFCParameters())


# COMMAND ----------

tmp.printSchema()

# COMMAND ----------

df = dfR04_hist_pd[['ID_GLOBAL']].drop_duplicates()
print(df.shape)

for variable in dfR04_hist_pd.columns:
  if variable != 'ID' and variable != 'ID_GLOBAL':
    print("#"*25)
    print(variable)
    tmp = dfR04_hist_pd.loc[:,['ID_GLOBAL', 'ID', variable]]
    tmp = extract_features(tmp,default_fc_parameters=fc_parameters, column_id = "ID_GLOBAL", column_sort = "ID", n_jobs = 12)
    df = df.merge(tmp.reset_index().rename(columns = {'id': 'ID_GLOBAL'}), how = 'inner', on ='ID_GLOBAL')
    print(df.shape)

# COMMAND ----------

tmp.head(2).reset_index().rename(columns = {'id': 'ID_GLOBAL'})

# COMMAND ----------



# COMMAND ----------

dfR04_hist_pd_features = extract_features(dfR04_hist_pd,default_fc_parameters=fc_parameters, column_id = "ID_GLOBAL", column_sort = "ID", n_jobs = 12)

# COMMAND ----------

mtxs = ['mean','max', 'std', 'sum'] #, 'mad','sem','skew']
cols = dfR04_hist_pd.columns
new_cols = []
for c in cols:
  for m in mtxs:
    if c != 'ID_GLOBAL':
      new_cols.append(c+'_'+m)
new_cols 
tmp = dfR04_hist_pd.groupby('ID_GLOBAL').agg(mtxs).reset_index()
tmp.columns = ['ID_GLOBAL']+new_cols

df = dfR04_hist_pd.loc[dfR04_hist_pd['ID'] == 0, :]
df = df.merge(tmp, how = 'inner', on = 'ID_GLOBAL')
print(df.shape)

# COMMAND ----------

### MARCA BUENO/MAL

# COMMAND ----------

# B.Periodo_Id as P, CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) AS ID, Deuda_DVencida, Deuda_DCastigo
dfR04_BM = spark.sql("""select A.Rut, A.Periodo_Id, 
                      MAX(CASE  WHEN B.Rut is null then -1
                                WHEN B.Deuda_DVencida + B.Deuda_DCastigo > 0 then 1
                                ELSE 0 end) AS BM
                        from dfSample AS A
                        LEFT JOIN
                        risgcred_db.lnd_deuda_cmf_tb AS B
                        ON A.rut = B.Rut
                        WHERE
                             CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) <= -1
                        and  CAST( months_between(A.Periodo_Id,B.Periodo_Id ) AS INT ) >= -12
                        and B.Deuda_DVigente  +  B.Deuda_DMora  +  B.Deuda_DVencida + B.Deuda_DCastigo + B.Disp_Lineas> 0
                        GROUP BY A.Rut, A.Periodo_Id """)
print((dfR04_BM.count(), len(dfR04_BM.columns)))
dfR04_BM_pd = dfR04_BM.select("*").toPandas()
dfR04_BM_pd.shape

# COMMAND ----------

# dfR04_BM_pd.sort_values(['Rut','Periodo_Id'], ascending = [True,True]).head(2)
dfR04_BM_pd.groupby('BM').size()

# COMMAND ----------

dfR04_BM_pd['ID_GLOBAL'] = dfR04_BM_pd['Rut'].astype(str) + ";" + dfR04_BM_pd['Periodo_Id'].astype(str)
dfR04_BM_pd.drop(['Rut','Periodo_Id'], axis = 1, inplace = True)
dfR04_BM_pd.head(3)

# COMMAND ----------

df = df.merge(dfR04_BM_pd, how ='left', on = 'ID_GLOBAL')
df.head(4)

# COMMAND ----------

def split_data(df,target): 
  X = df.drop(target, axis=1)
  y = df[target].values

  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
  data = {"train": {"X": X_train, "y": y_train},
         "test": {"X": X_test, "y": y_test}
        }
  return data

# COMMAND ----------

features = list(df.columns)
#drop_features = ['Creditability']
drop_features = ['ID_GLOBAL','ID','BM']

#Con un ciclo for se identifican los elementos de drop_features en features y se remueven
for f in drop_features:
    features.remove(f)
#Imprimo el total de variables seleccionadas
print("Total de variables candidatas: ", len(features))
#Creación una nueva variable que almacene el nuevo conjunto de datos
seleccionadas = features.copy()

# COMMAND ----------

from sklearn.metrics import roc_auc_score
from scipy.stats import ks_2samp


def evaluate(y_true,y_pred, verbose = 0):
    a1 = roc_auc_score(y_true,y_pred)
    a1 = max(a1,1-a1)
    a2 = ks_2samp(y_pred[y_true == 1], y_pred[y_true == 0])
    x = y_pred[y_true == 1]
    y = y_pred[y_true == 0]
    varM = np.var(x) + np.var(y)
    if varM > 0:
        a3 = (2 * (x.mean() - y.mean())**2) / (varM)
    else:
        a3 = -1
    if verbose > 0:
        print('ROC {:0.2f} | KS {:0.2f} | DIV {:0.2f} '.format(a1,a2.statistic, a3))
    r = (a1, a2.statistic, a3)
    return r

# COMMAND ----------

print(df.shape)
df = df.fillna(-1).loc[(df['Deuda_DVencida'] + df['Deuda_DCastigo']) == 0, :]
print(df.shape)
df = df.loc[df['BM'] >= 0, :]
print(df.shape)
data = split_data(df,target = 'BM')
print(data['train']['X'].shape)
print(data['test']['X'].shape)

# COMMAND ----------

predVar = []

for f in seleccionadas:
    print(f)
    metricas = evaluate(data['train']['y'],data['train']['X'].loc[:,f], verbose = 0)
    predVar.append([f, f.split("_")[0], metricas[0],metricas[1],metricas[2]])
predVar = pd.DataFrame(predVar, columns = ['Feature','Group','ROC','KS','DIV'])
predVar = predVar.loc[(predVar['KS'] > 0.01) & (predVar['ROC'] > 0.501),:]
predVar.head(10)

# COMMAND ----------

# conteo de nuevo subgrupo de variables (seleccion bivariada)

print("Total de variables candidatas: ", len(seleccionadas))
seleccionadas = list(predVar['Feature'])
print("Total de variables candidatas: ", len(seleccionadas))

# COMMAND ----------

#Seleccion por correlacion

corThr = 0.5
print("SELECCIONADAS PRE: ", len(seleccionadas))

seleccionadas = []
   
predVarFam = predVar.sort_values('KS',ascending = False)

variables_candidatas    = predVarFam['Feature']
variables_revisadas     = []
variables_seleccionadas = []
corMtx = data['train']['X'].loc[:,variables_candidatas].corr()

for f in variables_candidatas:
    if f not in variables_revisadas:
#         print("#"*10)
#         print(f)
        variables_revisadas.append(f)
        variables_seleccionadas.append(f)

        corTemp = list(corMtx.index[(np.abs(corMtx[f]) >= corThr)])
        for fx in corTemp:
            if fx != f:
                variables_revisadas.append(fx)

#         print(len(variables_revisadas))
#         print(len(variables_seleccionadas))
seleccionadas = seleccionadas + variables_seleccionadas

print("SELECCIONADAS POST: ", len(seleccionadas))

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

from lightgbm import LGBMClassifier
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from matplotlib import pyplot

parameters = {'n_estimators':[ 250, 500, 1000],
                      'max_features': ['auto','log2' ],
                      'max_depth': [4, 6, 8, 10],
                      'learning_rate':[0.01,0.05,0.1]
                      }
prob = 0.2
mask = np.random.binomial(1, prob, data['train']['X'].shape[0])
  
est = LGBMClassifier(tree_learner='data')
cv = 3
clf = GridSearchCV(est, parameters, cv=cv,  verbose = 1, n_jobs = 6, scoring = 'roc_auc')
clf.fit(data['train']['X'].loc[(mask==1),seleccionadas], data['train']['y'][(mask==1)])

##FIT BEST MODEL
print("LGBM",clf.best_params_)
clf = LGBMClassifier(n_estimators  = clf.best_params_['n_estimators'], 
                     max_depth     = clf.best_params_['max_depth'],
                     max_features  = clf.best_params_['max_features'],
                     learning_rate = clf.best_params_['learning_rate'],
                     tree_learner='data',
                     n_jobs = 4)

clf.fit(data['train']['X'].loc[:,seleccionadas], data['train']['y']) 

# COMMAND ----------

pred = {"train":   {"y_pred": clf.predict_proba(data['train']['X'].loc[:,seleccionadas])[:,1]},
        "test":    {"y_pred": clf.predict_proba(data['test']['X'].loc[:,seleccionadas])[:,1]},
       }

# COMMAND ----------

evaluate(data['train']['y'],  pred['train']['y_pred'], verbose = 1)
evaluate(data['test']['y'],   pred['test']['y_pred'], verbose = 1)

# COMMAND ----------

###IMPORTANCIA DE VARIABLES
headers = ["VARIABLE", "IMPORTANCE"]   
values  = sorted(zip(seleccionadas, clf.feature_importances_), key=lambda x: x[1] * -1)
values = pd.DataFrame(values, columns = headers)
values.sort_values('IMPORTANCE', ascending = False, inplace = True)
values

# COMMAND ----------

import shap
shap_values = shap.TreeExplainer(clf).shap_values(data['test']['X'].loc[:,seleccionadas])
shap.summary_plot(shap_values, data['test']['X'].loc[:,seleccionadas], 
                  max_display = 10,
                  plot_size = (14,4),
                  class_names = ['Non-Default', 'Default'],
                  plot_type="bar", show = False)
plt.tight_layout()
plt.show()

# COMMAND ----------

shap.summary_plot(shap_values[1], data['test']['X'].loc[:,seleccionadas], 
                          max_display = 10,
                          plot_size = (14,4),
                          show = False)
plt.tight_layout()
# plt.savefig('../RESULTS/Modelos/V0/SHAP_1_'+DATASET+'_'+FAM+'_'+trainModel+'.png')
# plt.clf()
# plt.close()

# COMMAND ----------

dfSII = spark.sql("select B.* from R04Sample AS A LEFT JOIN risgcred_db.tmp_sii_tb AS B ON A.cli_rut=B.cli_rut")
print((dfSII.count(), len(dfSII.columns)))

# COMMAND ----------

display(dfSII.take(3))

# COMMAND ----------



# COMMAND ----------

tmp = spark.sql("select periodo_id, count(1) AS N from risgcred_db.lnd_bien_raiz_tb GROUP BY periodo_id ORDER BY periodo_id")
print((tmp.count(), len(tmp.columns)))

# COMMAND ----------



# COMMAND ----------

dfBbrr = spark.sql("select B.* from R04Sample AS A LEFT JOIN risgcred_db.lnd_bien_raiz_tb AS B ON A.cli_rut=B.cli_rut")
print((dfBbrr.count(), len(dfBbrr.columns)))

# COMMAND ----------

display(dfBbrr.take(3))

# COMMAND ----------

dfR04 = spark.sql("select * from risgcred_db.tmp_sii_tb limit 1000")
display(dfR04.select("*"))