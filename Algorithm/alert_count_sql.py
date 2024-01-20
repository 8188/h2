cols_A = [
    "c0", "c1", "c42", "c43", "c44", "c45", "c46", "c89", "c94", "c95", "c96", "c97",
    "c98", "c99", "c100", "c101", "c102", "c103", "c104", "c105", "c106", "c107",
    "c108", "c109", "c110", "c111", "c112", "c113", "c114", "c115", "c116", 
    "c117", "c118", "c119", "c120", "c121", "c122", "c123", "c124", "c125", 
    "c126", "c127", "c128", "c129", "c130", "c131", "c132", "c133", "c134", 
    "c135", "c136", "c137", "c138", "c139", "c140", "c141", "c142", "c143", 
    "c144", "c145", "c146", "c147", "c148", "c149", "c150", "c151", "c152", 
    "c153", "c154", "c155", "c156", "c157", "c158", "c159", "c160", "c161", 
    "c162", "c163", "c164", "c165", "c166", "c167", "c168", "c169", "c170",
    "c171", "c172", "c173"
]

cols_PEM = [
    "c421", "c422", "c424", "c426", "c430", "c431", "c432", "c433", "c434", 
    "c435", "c436", "c437", "c438", "c439", "c440", "c441", "c442", "c443",
    "c444", "c445", "c446", "c447", "c448", "c449", "c464", "c465", "c466",
    "c467", "c468", "c469", "c470", "c471", "c472", "c473", "c474", "c475",
    "c476", "c477", "c478", "c479", "c480", "c481", "c482", "c483", "c484",
    "c485", "c486", "c487", "c488", "c489", "c490"
]

cols_PG = [
    "c357", "c359", "c360", "c362", "c363", "c364", "c365", "c366", "c367", 
    "c368", "c369", "c370", "c384", "c385", "c386", "c387", "c388", "c389",
    "c390", "c391", "c392"
]

alias_A = "`控制`"
alias_PEM = "`制氢`"
alias_PG = "`提纯`"

table = "h2.s_bool"
lastTime = "ts > now() - 1h"
firstTime = "ts > now() - 1n - 1h and ts < now() -1n"

def generate_sql(cols, alias, timeCondition=lastTime):
    diff_columns = ', '.join([f'diff({c}) as diff{c}' for c in cols])
    conditions = ' OR '.join([f'diff{c} = 1' for c in cols])
    
    query = f"SELECT COUNT(*) as {alias} FROM (SELECT ts, {diff_columns} FROM {table}) WHERE {conditions} and {timeCondition}"
    return query


if __name__ == "__main__":
    sql = generate_sql(cols_PG, alias_PG)
    print(sql)


# select count(*) from (select c0 from test_table state_window(c0)) where c0=true;
# select count(*) from (select diff(c0) as diffc0 from test_table) where diffc0=1;
# select count(*) from (select count(*) from test_table event_window start with c0=true end with c0=false);