import mysql.connector as mysql

def connect(password, db_name):
    try:
        return mysql.connect(
            host='localhost',
            user='root',
            password=password,
            database=db_name)
    except Error as e:
        print(e)
        
def createTableSQL (password, db_name) :
    
    #--------------------------------------------------------------------------------------------------------------------------------------------------
    ftable = "stock_fundamental"
    ovtable = "stock_overview"
    fintable = "financial_report"
    cashtable = "cashflow"
    incometable = "income_statement"
    balancetable = "balance_sheet"

    create_ovtable = ("CREATE TABLE IF NOT EXISTS {table} "
                      " ( SYMBOL VARCHAR(8) NOT NULL PRIMARY KEY, ASSET_TYPE VARCHAR(30), NAME VARCHAR(100) NOT NULL, "
                      " DESCRIPTION VARCHAR(5000), EXCHANGE VARCHAR(10), CURRENCY VARCHAR(5), "
                      " COUNTRY VARCHAR(50), SECTOR VARCHAR(50), INDUSTRY VARCHAR(50), "
                      " ADDRESS VARCHAR(1000), Full_Time_Employees  VARCHAR(15), Fiscal_Year_End VARCHAR(15), "
                      " LATEST_QUARTER VARCHAR(10), DividendDate VARCHAR(10), ExDividendDate VARCHAR(10), "
                      " LastSplitFactor VARCHAR(10), LastSplitDate VARCHAR(10) )"
                     )
    create_fintable = ("CREATE TABLE IF NOT EXISTS {table} "
                      " ( SYMBOL VARCHAR(8) PRIMARY KEY, MARKET_CAP VARCHAR(30), EBITDA VARCHAR(30), "
                      " PER VARCHAR(30), PEGR VARCHAR(30), BOOK_VALUE VARCHAR(30), "
                      " Dividend_Per_Share VARCHAR(30), Dividend_Yield VARCHAR(30), EPS VARCHAR(30), "
                      " Revenue_Per_Share_TTM VARCHAR(30), Profit_Margin VARCHAR(30), Operating_Margin_TTM VARCHAR(30), "
                      " ROA_TTM VARCHAR(30), ROE_TTM VARCHAR(30), REVENUE_TTM VARCHAR(30), "
                      " Gross_Profit_TTM VARCHAR(30), Diluted_EPS_TTM VARCHAR(30), Quarterly_Earnings_Growth_YOY VARCHAR(30), "
                      " Quarterly_Revenue_Growth_YOY VARCHAR(30), Analyst_Target_Price VARCHAR(30), Trailing_PE VARCHAR(30), "
                      " Forward_PE VARCHAR(30), Price_to_Sales_Ratio_TTM VARCHAR(30), PBV VARCHAR(30), "
                      " EVtoRevenue VARCHAR(30), EVtoEBITDA VARCHAR(30), Beta VARCHAR(30), "
                      " 52WeekHigh VARCHAR(30), 52WeekLow VARCHAR(30), 50DayMovingAverage VARCHAR(30), "
                      " 200DayMovingAverage VARCHAR(30), SharesOutstanding VARCHAR(30), SharesFloat VARCHAR(30), " 
                      " SharesShort VARCHAR(30), SharesShortPriorMonth VARCHAR(30), ShortRatio VARCHAR(30), " 
                      " ShortPercentOutstanding VARCHAR(30), ShortPercentFloat VARCHAR(30), PercentInsiders VARCHAR(30), "
                      " PercentInstitutions VARCHAR(30), ForwardAnnualDividendRate VARCHAR(30), ForwardAnnualDividendYield VARCHAR(30), " 
                      " PayoutRatio VARCHAR(30) ) "
                      )
    create_ftable = ("CREATE TABLE IF NOT EXISTS {table} "
                   " ( UNIQUE_ID VARCHAR(20) NOT NULL PRIMARY KEY, SYMBOL VARCHAR(8) NOT NULL, "
                   " DATE date NOT NULL, LAST_PRICE_RP decimal(8, 2) NOT NULL,"
                   " SHARE_OUT decimal(15, 2), MARKET_CAP_RP decimal(15, 2), "
                   " EPS decimal(15, 2),  EPSTTMM_RP decimal(15, 2), "
                   " PER_X decimal(15, 2), BVPS_RP decimal(15, 2), "
                   " PBV_X decimal(15, 2), ROA_PERCENT decimal(15, 2), "
                   " ROE_PERCENT decimal(15, 2), EV decimal(15, 2), "
                   " EBITDA decimal(15, 2), EV_EBITDA_RATIO decimal(15, 2), "
                   " D_E_RATIO decimal(15, 2), DEBT_TOTALCAP_RATIO decimal(15, 2), "
                   " DEBT_EBITDA_RATIO decimal(15, 2) ) "
                   )

    create_cashtable = ("CREATE TABLE IF NOT EXISTS {table} "
                        " ( uniqueId VARCHAR(20) NOT NULL PRIMARY KEY, symbol VARCHAR(8) NOT NULL, "
                        " fiscaldate date NOT NULL, reportedCurrency VARCHAR(5), operatingCashflow decimal(15, 2) NOT NULL,"
                        " paymentsForOperatingActivities decimal(15, 2) NOT NULL, proceedsFromOperatingActivities decimal(15, 2) NOT NULL, changeInOperatingLiabilities decimal(15, 2), "
                        " changeInOperatingAssets decimal(15, 2) NOT NULL, depreciationDepletionAndAmortization decimal(15, 2) NOT NULL, capitalExpenditures decimal(15, 2), "
                        " changeInReceivables decimal(15, 2), changeInInventory decimal(15, 2), profitLoss decimal(15, 2), "
                        " cashflowFromInvestment decimal(15, 2), cashflowFromFinancing decimal(15, 2), proceedsFromRepaymentsOfShortTermDebt decimal(15, 2), "
                        " paymentsForRepurchaseOfCommonStock decimal(15, 2), paymentsForRepurchaseOfEquity decimal(15, 2), paymentsForRepurchaseOfPreferredStock decimal(15, 2), "
                        " dividendPayout decimal(15, 2), dividendPayoutCommonStock decimal(15, 2), dividendPayoutPreferredStock decimal(15, 2), "
                        " proceedsFromIssuanceOfCommonStock decimal(15, 2), proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet decimal(15, 2), proceedsFromIssuanceOfPreferredStock decimal(15, 2), "
                        " proceedsFromRepurchaseOfEquity decimal(15, 2), proceedsFromSaleOfTreasuryStock decimal(15, 2), changeInCashAndCashEquivalents decimal(15, 2), "
                        " changeInExchangeRate decimal(15, 2), netIncome decimal(15, 2) )"
                       )
    create_incometable = ("CREATE TABLE IF NOT EXISTS {table} "
                          " ( uniqueId VARCHAR(20) NOT NULL PRIMARY KEY, symbol VARCHAR(8) NOT NULL, "
                          " fiscaldate date NOT NULL, reportedCurrency VARCHAR(5), grossProfit decimal(15, 2) NOT NULL, "
                          " totalRevenue decimal(15, 2) NOT NULL, costOfRevenue decimal(15, 2) NOT NULL,  costofGoodsAndServicesSold decimal(15, 2) NOT NULL, "
                          " operatingIncome decimal(15, 2), sellingGeneralAndAdministrative decimal(15, 2), researchAndDevelopment decimal(15, 2), "
                          " operatingExpenses decimal(15, 2), investmentIncomeNet decimal(15, 2), netInterestIncome decimal(15, 2), "
                          " interestIncome decimal(15, 2), interestExpense decimal(15, 2), nonInterestIncome decimal(15, 2), "
                          " otherNonOperatingIncome decimal(15, 2), depreciation decimal(15, 2), depreciationAndAmortization decimal(15, 2), "
                          " incomeBeforeTax decimal(15, 2), incomeTaxExpense decimal(15, 2), interestAndDebtExpense decimal(15, 2), "
                          " netIncomeFromContinuingOperations decimal(15, 2), comprehensiveIncomeNetOfTax decimal(15, 2), ebit decimal(15, 2), "
                          " ebitda decimal(15, 2), netIncome decimal(15, 2) )"
                        )       
    create_balancetable = ("CREATE TABLE IF NOT EXISTS {table} "
                           " ( uniqueId VARCHAR(20) NOT NULL PRIMARY KEY, symbol VARCHAR(8) NOT NULL, "
                           " fiscaldate date NOT NULL, reportedCurrency VARCHAR(5), totalAssets decimal(15, 2) NOT NULL, "
                           " totalCurrentAssets decimal(15, 2), cashAndCashEquivalentsAtCarryingValue decimal(15, 2), cashAndShortTermInvestments decimal(15, 2), "
                           " inventory decimal(15, 2), currentNetReceivables decimal(15, 2), totalNonCurrentAssets decimal(15, 2), "
                           " propertyPlantEquipment decimal(15, 2), accumulatedDepreciationAmortizationPPE decimal(15, 2), intangibleAssets decimal(15, 2), "
                           " intangibleAssetsExcludingGoodwill decimal(15, 2), goodwill decimal(15, 2), investments decimal(15, 2), "
                           " longTermInvestments decimal(15, 2), shortTermInvestments decimal(15, 2), otherCurrentAssets decimal(15, 2), "
                           " otherNonCurrrentAssets decimal(15, 2), totalLiabilities decimal(15, 2), totalCurrentLiabilities decimal(15, 2), "
                           " currentAccountsPayable decimal(15, 2), deferredRevenue decimal(15, 2), currentDebt decimal(15, 2), "
                           " shortTermDebt decimal(15, 2), totalNonCurrentLiabilities decimal(15, 2), capitalLeaseObligations decimal(15, 2), "
                           " longTermDebt decimal(15, 2), currentLongTermDebt decimal(15, 2), longTermDebtNoncurrent decimal(15, 2), "
                           " shortLongTermDebtTotal decimal(15, 2), otherCurrentLiabilities decimal(15, 2), otherNonCurrentLiabilities decimal(15, 2), "
                           " totalShareholderEquity decimal(15, 2), treasuryStock decimal(15, 2), retainedEarnings decimal(15, 2), "
                           " commonStock decimal(15, 2), commonStockSharesOutstanding decimal(15, 2) ) "
                          )
    
    db = connect(password, db_name)
    cursor = db.cursor()
    
    try:
        cursor.execute(create_ovtable.format(table=ovtable) )                    
        cursor.execute(create_fintable.format(table=fintable) )                
        cursor.execute(create_ftable.format(table=ftable) )
        cursor.execute(create_cashtable.format(table=cashtable) )                    
        cursor.execute(create_incometable.format(table=incometable) )                
        cursor.execute(create_balancetable.format(table=balancetable) )
        
    except mysql.Error as err:
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    db.commit()
    db.close()  

def insertTableSQL (password, db_name, entity, Calculation, CFQ_reader, ISQ_reader, BSQ_reader, OV_reader) :
    #--------------------------------------------------------------------------------------------------------------------------------------------------
    ftable = "stock_fundamental"
    ovtable = "stock_overview"
    fintable = "financial_report"
    cashtable = "cashflow"
    incometable = "income_statement"
    balancetable = "balance_sheet"
    
    db = connect(password, db_name)
    cursor = db.cursor()
    
    mysql_list = []
    Unique_ID  = []
    StockName  = [entity.upper()]*len(Calculation['Date'])  

    for i in range (len(Calculation['Date'])) :
        temp = entity + str(Calculation['Date'][i].strftime('%Y%m%d'))
        Unique_ID.append(temp)
        i += 1

    mysql_list.append(Unique_ID)
    mysql_list.append(StockName)

    for i in list(Calculation.columns) :
        mysql_list.append(Calculation[i])
        
    #------------------------------------------------------------------------------------------
    #inserting data overview into mysql
    insert_ovtable = ("REPLACE INTO {table} "
                    "( SYMBOL, ASSET_TYPE, "
                    "NAME, DESCRIPTION, EXCHANGE, "
                    "CURRENCY, COUNTRY, SECTOR, "
                    "INDUSTRY, ADDRESS, Full_Time_Employees, "
                    "Fiscal_Year_End, LATEST_QUARTER, DividendDate, "
                    "ExDividendDate, LastSplitFactor, LastSplitDate ) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                     )

    list_header_OV = OV_reader.index.tolist()

    temp_data = []

    for i in range(13) :
        single_data = OV_reader[list_header_OV[i]]
        temp_data.append(single_data)
    for i in range(55, len(list_header_OV)) :
        single_data = OV_reader[list_header_OV[i]]
        temp_data.append(single_data)
    temp_data = [str(s) for s in temp_data ]

    try :
        cursor.execute(insert_ovtable.format(table=ovtable), temp_data)
    except mysql.Error as err:
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    ##############################
    #inserting financial report into mysql
    insert_fintable = ("REPLACE INTO {table} "
                        "( SYMBOL, MARKET_CAP, EBITDA, " 
                        " PER, PEGR, BOOK_VALUE, "
                        " Dividend_Per_Share, Dividend_Yield, EPS, "
                        " Revenue_Per_Share_TTM, Profit_Margin, Operating_Margin_TTM, "
                        " ROA_TTM, ROE_TTM, REVENUE_TTM, "
                        " Gross_Profit_TTM, Diluted_EPS_TTM, Quarterly_Earnings_Growth_YOY, "
                        " Quarterly_Revenue_Growth_YOY, Analyst_Target_Price, Trailing_PE, "
                        " Forward_PE, Price_to_Sales_Ratio_TTM, PBV, "
                        " EVtoRevenue, EVtoEBITDA, Beta, "
                        " 52WeekHigh, 52WeekLow, 50DayMovingAverage, "
                        " 200DayMovingAverage, SharesOutstanding, SharesFloat, "
                        " SharesShort, SharesShortPriorMonth, ShortRatio, "
                        " ShortPercentOutstanding, ShortPercentFloat, PercentInsiders, "
                        " PercentInstitutions, ForwardAnnualDividendRate, ForwardAnnualDividendYield, PayoutRatio ) "
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                        " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
                      )
    temp_data = []
    temp_data.append(entity)
    for i in range(13, 55) :
        single_data = OV_reader[list_header_OV[i]]
        temp_data.append(single_data)
    temp_data = [str(s) for s in temp_data ]

    try :
        cursor.execute(insert_fintable.format(table=fintable), temp_data)
    except mysql.Error as err:
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

    #inserting data fundamental into mysql
    insert_ftable = ("REPLACE INTO {table} "
                   "(UNIQUE_ID, SYMBOL, DATE, LAST_PRICE_RP, SHARE_OUT, MARKET_CAP_RP, "
                   "EPS, EPSTTMM_RP, PER_X, BVPS_RP, PBV_X, ROA_PERCENT, ROE_PERCENT, "
                   "EV, EBITDA, EV_EBITDA_RATIO, D_E_RATIO, DEBT_TOTALCAP_RATIO, "
                   "DEBT_EBITDA_RATIO) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    j = 0
    while j < len(mysql_list[0]) :
        temp_data = []
        i = 0
        while i < len(mysql_list) :
            temp_data.append(str (mysql_list[i][j]))
            #print(project_data)
            i += 1

        cursor.execute(insert_ftable.format(table=ftable), temp_data)
        j += 1

    ##############################    
    #dataframe to list
    cashtable_list = []
    Unique_ID  = []
    StockName = []
    StockName  = [entity.upper()]*len (CFQ_reader["fiscalDateEnding"]) 

    for i in range (len (CFQ_reader["fiscalDateEnding"]) ) :
        temp = entity + str(CFQ_reader["fiscalDateEnding"][i].strftime('%Y%m%d'))
        Unique_ID.append(temp)
        i += 1

    cashtable_list.append(Unique_ID)
    cashtable_list.append(StockName)

    for i in list(CFQ_reader.columns) :
        cashtable_list.append(CFQ_reader[i])  

    #inserting data cash flow into mysql
    insert_cashtable = ("REPLACE INTO {table} "
                        " ( uniqueId, symbol, "
                        " fiscalDate, reportedCurrency, operatingCashflow, "
                        " paymentsForOperatingActivities, proceedsFromOperatingActivities, changeInOperatingLiabilities, "
                        " changeInOperatingAssets, depreciationDepletionAndAmortization, capitalExpenditures, "
                        " changeInReceivables, changeInInventory, profitLoss, "
                        " cashflowFromInvestment, cashflowFromFinancing, proceedsFromRepaymentsOfShortTermDebt, "
                        " paymentsForRepurchaseOfCommonStock, paymentsForRepurchaseOfEquity, paymentsForRepurchaseOfPreferredStock, "
                        " dividendPayout, dividendPayoutCommonStock, dividendPayoutPreferredStock, "
                        " proceedsFromIssuanceOfCommonStock, proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet, proceedsFromIssuanceOfPreferredStock, "
                        " proceedsFromRepurchaseOfEquity, proceedsFromSaleOfTreasuryStock, changeInCashAndCashEquivalents, "
                        " changeInExchangeRate, netIncome ) "
                        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" )             

    #inserting to mysql
    j = 0
    while j < len(cashtable_list[0]) :
        temp_data = []
        i = 0
        while i < len(cashtable_list) :
            temp_data.append(str (cashtable_list[i][j]))
            #print(project_data)
            i += 1
        try :
            cursor.execute(insert_cashtable.format(table=cashtable), temp_data)
        except mysql.Error as err:
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        j += 1
    ##############################
    #dataframe to list
    incometable_list = []
    Unique_ID  = []
    StockName = []
    StockName  = [entity.upper()]*len (ISQ_reader["fiscalDateEnding"]) 

    for i in range (len (ISQ_reader["fiscalDateEnding"]) ) :
        temp = entity + str(ISQ_reader["fiscalDateEnding"][i].strftime('%Y%m%d'))
        Unique_ID.append(temp)
        i += 1

    incometable_list.append(Unique_ID)
    incometable_list.append(StockName)

    for i in list(ISQ_reader.columns) :
        incometable_list.append(ISQ_reader[i])  

    #inserting incometable into mysql
    insert_incometable = ("REPLACE INTO {table} "
                          " ( uniqueId, symbol, "
                          " fiscalDate, reportedCurrency, grossProfit, "
                          " totalRevenue, costOfRevenue, costofGoodsAndServicesSold, "
                          " operatingIncome, sellingGeneralAndAdministrative, researchAndDevelopment, "
                          " operatingExpenses, investmentIncomeNet, netInterestIncome, "
                          " interestIncome, interestExpense, nonInterestIncome, "
                          " otherNonOperatingIncome, depreciation, depreciationAndAmortization, "
                          " incomeBeforeTax, incomeTaxExpense, interestAndDebtExpense, "
                          " netIncomeFromContinuingOperations, comprehensiveIncomeNetOfTax, ebit, "
                          " ebitda, netIncome ) "
                          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" )

    #inserting to mysql
    j = 0
    while j < len(incometable_list[0]) :
        temp_data = []
        i = 0
        while i < len(incometable_list) :
            temp_data.append(str (incometable_list[i][j]))
            #print(project_data)
            i += 1

        try :
            cursor.execute(insert_incometable.format(table=incometable), temp_data)
        except mysql.Error as err:
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        j += 1

    ##############################
    #dataframe to list
    balancetable_list = []
    Unique_ID  = []
    StockName = []
    StockName  = [entity.upper()]*len (BSQ_reader["fiscalDateEnding"]) 

    for i in range (len (BSQ_reader["fiscalDateEnding"]) ) :
        temp = entity + str(BSQ_reader["fiscalDateEnding"][i].strftime('%Y%m%d'))
        Unique_ID.append(temp)
        i += 1

    balancetable_list.append(Unique_ID)
    balancetable_list.append(StockName)

    for i in list(BSQ_reader.columns) :
        balancetable_list.append(BSQ_reader[i])  

    #inserting incometable into mysql
    insert_balancetable = ("REPLACE INTO {table} "
                            " ( uniqueId, symbol, "
                            " fiscalDate, reportedCurrency, totalAssets, "
                            " totalCurrentAssets, cashAndCashEquivalentsAtCarryingValue, cashAndShortTermInvestments, "
                            " inventory, currentNetReceivables, totalNonCurrentAssets, "
                            " propertyPlantEquipment, accumulatedDepreciationAmortizationPPE, intangibleAssets, "
                            " intangibleAssetsExcludingGoodwill, goodwill, investments, "
                            " longTermInvestments, shortTermInvestments, otherCurrentAssets, "
                            " otherNonCurrrentAssets, totalLiabilities, totalCurrentLiabilities, "
                            " currentAccountsPayable, deferredRevenue, currentDebt, "
                            " shortTermDebt, totalNonCurrentLiabilities, capitalLeaseObligations, "
                            " longTermDebt, currentLongTermDebt, longTermDebtNoncurrent, "
                            " shortLongTermDebtTotal, otherCurrentLiabilities, otherNonCurrentLiabilities, "
                            " totalShareholderEquity, treasuryStock, retainedEarnings, "
                            " commonStock, commonStockSharesOutstanding ) "
                            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                            " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" )

    #inserting to mysql
    j = 0
    while j < len(balancetable_list[0]) :
        temp_data = []
        i = 0
        while i < len(balancetable_list) :
            temp_data.append(str (balancetable_list[i][j]))
            #print(project_data)
            i += 1
        try :
            cursor.execute(insert_balancetable.format(table=balancetable), temp_data)
        except mysql.Error as err:
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
        j += 1


    db.commit()
    db.close()  

if __name__ == "__main__": 
    createTableSQL("minyak23","newstockmarket")
    
    