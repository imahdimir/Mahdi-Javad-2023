"""

    """
import re
from pathlib import Path
import os

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.ns import update_ns_module
from pyjarowinkler import distance

##
cwd = Path('ticker_match')
os.chdir(cwd)

update_ns_module()
from ticker_match import ns as ns

gdu = ns.GDU()
c = ns.Col()

fp = Path('scores_with_firmticker.csv')

##
def find_similar_ticker_in_df(df , tic) :
    df['srch'] = tic
    fu = lambda x : distance.get_jaro_distance(tic , x , winkler = True)
    df['sim'] = df[c.name].apply(fu)
    df = df.sort_values(by = 'sim' , ascending = False)
    df = df.reset_index(drop = True)
    return df

def main() :
    pass

    ##
    gdsn2f = GitHubDataRepo(gdu.src_n2f)
    gdsn2f.clone_overwrite()

    ##
    dfa = gdsn2f.read_data()

    ##
    dfb = pd.read_csv(fp)

    ##
    dfb = dfb[[c.tic , c.ftic]]
    dfb = dfb.drop_duplicates()

    ##
    msk = dfb[c.ftic].isna()
    dfb = dfb[msk]

    ##
    dfb = dfb.drop_duplicates(subset = c.tic)

    ##
    df = pd.DataFrame(columns = [c.name , c.ftic])

    ##
    for i , row in dfb.iterrows() :
        tic = row[c.tic]
        df1 = find_similar_ticker_in_df(dfa , tic)

        print(tic)
        print(df1[[c.name , c.ftic]].head(10))

        ind = input('Enter Index: ')
        if re.match(r'^\d+$' , ind) :
            ind = int(ind)
            df2 = pd.DataFrame({
                    c.name : [tic] ,
                    c.ftic : [df1.iloc[ind][c.name]]
                    })
            df = pd.concat([df , df2] , ignore_index = True)

        df.to_parquet('temp.prq' , index = False)

    ##

##

if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
