# Get all top-level classes/functions. import them from  api.py from lower-level modules.

def test_df():
    import biz.pandas as bp
    #df = bp.read_file(r"P:\data\source\kpn\swol_marketing\google\2010-03-23\s1119446a - batch 1 - 50 000 adressen_sbi2.csv")
    df = bp.read_source("google", "s1119446a - batch 1 - 50 000 adressen_sbi2.csv")[-1]
    df.ix[::1000].to_csv(r"P:\data\source\kpn\swol_marketing\google\2010-03-23\s1119446a - batch 1 - 50 000 adressen_sbi2-test.csv")

if __name__ == '__main__':
    test_df()