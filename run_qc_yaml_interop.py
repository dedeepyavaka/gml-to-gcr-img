from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
import argparse,os
import pandas as pd
import yaml
import pickle

#get_interop_info
def get_columns_name():
    """
    :return: a dictionary of tuple for column information ("dataframe Name","Interop Name")
    """

    base_columns = (('lane', 'lane'),)
    xread_columns = (('phasing', 'phasing'), ('pre_phasing', 'prephasing'))
    lane_columns = (
        ('error', 'error_rate'), ('cluster_density', 'density'), ('cluster_density_passing_filter', 'density_pf'),
        ('reads_passing_filter', 'reads_pf'), ('reads', 'reads'), ('quality', 'percent_gt_q30'))
    read_columns = (('reads_aligned', 'percent_aligned'),)
    read_yield_columns = (('yield', 'yield_g'),)
    scope = locals()
    col_dict = { i:eval(i,scope) for i in [ "base_columns" , "lane_columns" , "read_columns" , "read_yield_columns" , "xread_columns"]}
    col_dict["summary_columns"] = base_columns + lane_columns + read_columns + read_yield_columns + xread_columns
    #columns for index_lane summary
    col_dict["idx_columns"] = (('sample_name', 'sample_id'), ('index1', 'index1'), ('index2', 'index2'),('index_reads_passing_filter', 'fraction_mapped'))
    # columns for summary read dataframe
    col_dict["summary_read_columns"] = (("is_index", "is_index"), ('number_of_cycles', 'total_cycles'))
    return col_dict
def format_value(val):
    """
    helper function to parse the interop object that has a mean attribute
    :param obj val: interop object
    :return: val
    """

    if hasattr(val, 'mean'):
        return val.mean()
    else:
        return val

def parse_sav_summary(summary,columns):
    """
     :param interop.run_summary summary: interop run_summary object
     :param tuple columns: a tuple of tuple of two elements (sav_summary_atr_name,df_col_name)
     :return pandas.DataFrame summary_df: pandas DataFrame of the selected information

     """
    summary_df = pd.DataFrame()
    for n in range(0, summary.size()):
        read = n
        rows = [summary.at(read).at(lane) for lane in range(summary.lane_count())]
        d = []
        for label, func in columns:
            d.append((label, pd.Series([format_value(getattr(r, func)()) for r in rows])))
        df = pd.DataFrame.from_dict(dict(d))
        df["read_number"] = n + 1
        summary_df = summary_df.append(df, ignore_index=True) if len(summary_df) > 0 else df
    return summary_df.fillna(0)

def parse_index_summary(idx_summary,columns):

    """

    :param interop.index_lane_summary idx_summary: Interop index lane summary object
    :param tuple columns: a tuple of tuple of two elements (index_summary_atr_name,df_col_name)
    :return ind_df

    """
    ind_df=pd.DataFrame()
    for lane_n in range(0,idx_summary.size()):
        lane_summary = idx_summary.at(lane_n)
        d = []
        for label, func in columns:
            d.append( (label, pd.Series([getattr(lane_summary.at(i), func)() for i in range(lane_summary.size())], index=[lane_summary.at(i).id() for i in range(lane_summary.size())])))
        df = pd.DataFrame.from_dict(dict(d))
        df["index"]=df[["index1","index2"]].apply( lambda x: "-".join([x[0],x[1]]), axis=1)
        df["lane"]= lane_n+1
        df=df.drop(["index1","index2"],axis=1)
        ind_df = ind_df.append(df,ignore_index=True) if len(ind_df) >0 else df
    return ind_df.fillna(0)

def parse_summary_read_df(summary,columns):
    ''' parsing  Interop into pandas DataFrame'''
    summary_read_df = pd.DataFrame()
    d = {}
    for n in range(0, summary.size()):
        add = {n + 1: [getattr(summary.at(n).read(), func)() for label, func in columns]}
        d.update(add)
    summary_read_df = pd.DataFrame.from_dict(d, orient='index')
    summary_read_df.columns = [i[0] for i in columns]
    return summary_read_df.fillna(0)

def get_interop_info(run_path,col_dict):
    """
    :param str run_path: Folder path that contains InterOP folder and run_info.xml
    :return dict dfs: a dict of pandas dataframe
    """
    dfs={}
    #loading Interop run_metrics object
    run_metrics = py_interop_run_metrics.run_metrics()
    valid_to_load = py_interop_run.uchar_vector(py_interop_run.MetricCount, 0)
    py_interop_run_metrics.list_index_metrics_to_load(valid_to_load)
    # run_folder = run_metrics.read(run_path, valid_to_load)
    dfs["run_folder"] = run_metrics.read(run_path)
    #get number of run cycles
    dfs["number_of_cycle"] = run_metrics.run_info().useable_cycles()
    #get flowcell id name
    dfs["flowcell_id"]=run_metrics.run_info().name()
    #loading SAV summary object
    summary = py_interop_summary.run_summary()
    py_interop_summary.summarize_run_metrics(run_metrics, summary)
    summary_columns=col_dict["summary_columns"]
    dfs["summary_df"]= parse_sav_summary(summary,summary_columns)

    #parsing summary_read_df
    summary_read_columns=col_dict["summary_read_columns"]
    summary_read_df=parse_summary_read_df(summary,summary_read_columns)
    dfs["summary_read_df"]= summary_read_df
    #loading index-summary
    idx_summary = py_interop_summary.index_flowcell_summary()
    #parsing index-summary df 
    py_interop_summary.summarize_index_metrics(run_metrics, idx_summary)
    idx_columns=col_dict["idx_columns"]
    dfs["idx_df"]= parse_index_summary(idx_summary,idx_columns)

    #generating xread_df from summary_df
    xread_columns= col_dict["xread_columns"]
    dfs["xread_df"] = dfs["summary_df"][[i[0] for i in xread_columns] + ['read_number', 'quality']].groupby(['read_number']).mean()

    return dfs

#parse_interop_info 

def get_metrics():
    metrics_unit = {"index_reads_passing_filter": "%", "reads_aligned": "%", "quality": "%", "error": "%",
                    "reads": "Mbp", "reads_passing_filter": "Mbp", "cluster_density": "Kbp/mm^2",
                    "undetermined_indices": "%", "cluster_density_passing_filter": "%", "yield": "Gbp"}

    metrics_convert = {"reads_aligned": {"method": "*", "value": 10 ** 2},
                       "cluster_density": {"method": "/", "value": 10 ** 3},
                       "cluster_density_passing_filter": {"method": "*", "value": 10 ** 2},
                       "reads": {"method": "/", "value": 10 ** 6},
                       "reads_passing_filter": {"method": "/", "value": 10 ** 6}}
    return {"metrics_unit": metrics_unit,"metrics_convert":metrics_convert}

def add_unit(df_col,metrics_unit):
    if df_col.name in metrics_unit:
        unit = metrics_unit[df_col.name]
        return df_col.apply(lambda x: {"value":str(x), "units": unit})
    else:
        return df_col
def calc(v1,v2,method):
    try:
        result={
        '+': lambda x,y: x + y,
        '-': lambda x,y: x - y,
        '*': lambda x,y: x * y,
        '/': lambda x,y: x/y
        }[method](v1,v2)
        return str(result)
    except KeyError:
        raise Exception(" only accept calc methods:'+','-','*','/' ")

def convert_unit_value(df_col,metrics_convert):
    ''' only for simple arithmetic calculation. '''

    if df_col.name in metrics_convert:
        method = metrics_convert[df_col.name]["method"]
        value =  metrics_convert[df_col.name]["value"]
        # hmm...I will regret writing codes like this
        return df_col.apply(lambda x: calc(x,value,method))
    else:
        return df_col

def get_lane_level_metrics(summary_df, idx_df, col_dict, metrics_dict):
    base_columns = col_dict["base_columns"]
    lane_columns = col_dict["lane_columns"]
    metrics_convert = metrics_dict["metrics_convert"]
    metrics_unit = metrics_dict["metrics_unit"]
    lane_col = [i[0] for i in base_columns + lane_columns]
    lane_df = summary_df[lane_col].groupby(['lane']).mean()
    lane_df[lane_df.index.name] = lane_df.index
    lane_df["undetermined_indices"] = 100 - idx_df.groupby('lane').sum()["index_reads_passing_filter"]
    lane_df["cluster_density_passing_filter"] = lane_df["cluster_density_passing_filter"] / lane_df["cluster_density"]
    lane_df = lane_df.apply(convert_unit_value, metrics_convert=metrics_convert)
    lane_df = lane_df.apply(add_unit, metrics_unit=metrics_unit)
    return lane_df

def get_xread_level_metrics(xread_df,summary_read_df):
    # unit is not added for qulaity because the orignal yaml spec doesn't have it
    xread_df = xread_df.merge(summary_read_df, left_index=True, right_index=True)
    xread_df[xread_df.index.name] = xread_df.index
    return xread_df

def get_read_level_metrics(summary_df,col_dict, metrics_dict):
    base_columns = col_dict["base_columns"]
    read_columns = col_dict["read_columns"]
    metrics_convert = metrics_dict["metrics_convert"]
    metrics_unit = metrics_dict["metrics_unit"]
    read_col = [i[0] for i in base_columns + read_columns] + ["read_number"]
    read_df = summary_df[read_col]
    read_df = read_df.apply(convert_unit_value, metrics_convert=metrics_convert)
    read_df = read_df.apply(add_unit, metrics_unit=metrics_unit)
    return read_df

def get_read_yield_metrics(summary_df,col_dict,metrics_dict):
    base_columns = col_dict["base_columns"]
    read_yield_columns = col_dict["read_yield_columns"]
    metrics_convert = metrics_dict["metrics_convert"]
    metrics_unit = metrics_dict["metrics_unit"]
    read_yield_col = [i[0] for i in base_columns + read_yield_columns] + ["read_number"]
    read_yield_df = summary_df[read_yield_col]
    read_yield_df = read_yield_df.apply(convert_unit_value, metrics_convert=metrics_convert)
    read_yield_df = read_yield_df.apply(add_unit, metrics_unit=metrics_unit)
    return read_yield_df

def get_sample_level_metrics(ind_df, metrics_dict):
    metrics_convert = metrics_dict["metrics_convert"]
    metrics_unit = metrics_dict["metrics_unit"]
    ind_df = ind_df.apply(convert_unit_value, metrics_convert=metrics_convert)
    ind_df = ind_df.apply(add_unit, metrics_unit=metrics_unit)
    ind_df['sample_name'] = ind_df['sample_name'].str.replace('Sample_CGP-', 'CGP-')
    return ind_df


def parse_interop_info(interop_dfs,col_dict):
    final_summary_dict={}
    metrics_dict=get_metrics()
    summary_df= interop_dfs["summary_df"]
    idx_df = interop_dfs["idx_df"]
    summary_read_df = interop_dfs["summary_read_df"]
    xread_df = interop_dfs["xread_df"]

    final_summary_dict["lane_level_metrics"] = get_lane_level_metrics(summary_df, idx_df, col_dict, metrics_dict)
    final_summary_dict["xread_level_metrics"] = get_xread_level_metrics(xread_df, summary_read_df)
    final_summary_dict["read_level_metrics"] = get_read_level_metrics(summary_df,col_dict, metrics_dict)
    final_summary_dict["read_yield_metrics"] = get_read_yield_metrics(summary_df,col_dict, metrics_dict)
    final_summary_dict["sample_level_metrics"] = get_sample_level_metrics(idx_df,metrics_dict)
    final_summary_dict["run_level_metrics"] = pd.DataFrame(data={"number_of_cycles":[str(interop_dfs["number_of_cycle"])]})
    # uncomment below to add flowcell id
    final_summary_dict["run_flowcell_info"]=pd.DataFrame(data={"flowcell_id":[interop_dfs["flowcell_id"]]})

    return final_summary_dict

# files output

def make_yaml(summary_dict,out_file):
    summary_dict={ key:df.to_dict(orient='records') for key,df in summary_dict.items()}
    #print(summary_dict)
    with open( out_file, "w") as outfile:
        yaml.dump(summary_dict, outfile,
              default_flow_style=False,
              explicit_start=True
             )

def remove_unit(cell):
    try:
        return float(cell["value"])
    except:
        return cell

def get_avg_metrics(summary_dict):
    avg_lane_df= summary_dict["lane_level_metrics"].applymap(remove_unit)
    avg_lane_df= avg_lane_df.mean()
    avg_lane_df["lane"] = "Calculation"
    #total avg read per wetlab request
    avg_lane_df["reads"]= avg_lane_df["reads"]*2
    summary_dict["lane_level_metrics"]= summary_dict["lane_level_metrics"].append(avg_lane_df,ignore_index=True)

    return summary_dict


def make_csv(summary_dict,out_file):
    with open(out_file, "a") as outfile:
        for title,df in summary_dict.items():
            outfile.write(title+"\n")
            df.applymap(remove_unit).to_csv(outfile,index=False)
            outfile.write('\n')

def make_html(summary_dict,out_file):
    with open(out_file, "a") as outfile:
        for title,df in summary_dict.items():
            outfile.write(title+'<br>')
            df.applymap(remove_unit).to_html(outfile, index=False)
            outfile.write('<br></br>')
def make_cp(summary_dict,out_file):
    pickle.dump(summary_dict,open(out_file,"wb"))

def execute(target_dir, out_dir):

    col_dict=get_columns_name()
    dfs= get_interop_info(target_dir,col_dict)
    final_summary_dict=parse_interop_info(dfs,col_dict)
    out_yaml= os.path.join(out_dir ,'run_qc_metrics.yaml')
    out_csv= os.path.join(out_dir ,'run_qc_metrics.txt')
    out_html= os.path.join(out_dir,'run_qc_metrics.html')
    out_cp= os.path.join(out_dir,'run_qc_metrics.cp')
    make_yaml(final_summary_dict, out_yaml)
    lab_final_sumamry_dict =  final_summary_dict.copy()
    lab_final_sumamry_dict = get_avg_metrics(lab_final_sumamry_dict)
    make_csv(lab_final_sumamry_dict,out_csv)
    #print(final_summary_dict)
    make_html(lab_final_sumamry_dict,out_html)
    make_cp(lab_final_sumamry_dict,out_cp)

if __name__ == '__main__':
    AP = argparse.ArgumentParser(description=__doc__)
    AP.add_argument("target_dir", default=".",
                    help="""Directory that should contain:
                    (1) a RunInfo.xml file, and
                    (2) an 'InterOp' subdirectory containing Illumina .bin
                    files.""")
    AP.add_argument("outdir", default='.',
                    help="""Output directory.
                    Default: print to current directory.""")
    args = AP.parse_args()
    execute(args.target_dir, args.outdir)
