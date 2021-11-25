// This program backtest complex option positions
// It uses option price data from CBOE Datashop
// it gets SP500 dividend yield data from Quandl
// It gets Risk Free Interest rates from FRED
// It uses my modified version of Jaeckel's Lets Be Rational C++ program to compute option greeks

// This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis

#define NO_CALLS
#define ONLY25STRIKES
#undef PARFOR_READDATA
#undef PARFOR_ANALYZE

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using LetsBeRationalLib;
using System.Globalization;
using System.Diagnostics;
using System.Linq;
using ReadFredTreasuryRates;
using Npgsql; // for postgres

namespace OptionBacktester
{
    using StrikeIndex = SortedList<int, OptionData>; // index is strike
    using DeltaIndex = SortedList<int, OptionData>; // index is delta*10000, a delta of -0.05 for a put has a delta index of -.05*10000 = -500
    using ExpirationDate = DateTime;
    using Day = DateTime;
    using Time = DateTime;
    using SortedListExtensions;
    using System.Net.Http;

    class Option
    {
        internal string root;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal float multiplier = 100f; // converts option prices to dollars
        internal SortedList<DateTime, OptionData> optionData = new SortedList<DateTime, OptionData>();
    }

    class OptionData
    {
        //internal Option option;
        internal DateTime dt;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal string root;
        internal float underlying;
        internal float bid;
        internal float ask;
        internal float iv;
        internal float delta;
        internal float gamma;
        internal float theta;
        internal float vega;
        internal float rho;
        internal int openInterest;
        internal float riskFreeRate;
        internal float dividendYield;
        internal float mid;
        internal int dte;
        // delta100 is delta in percent times 100; int so it makes a good index; so, if delta is read as -0.5 (at the money put), it will have a delta100 of -5000
        internal int delta100 = -10000;
    }

    // for reading CBOE Data
    public enum CBOEFields : int
    {
        UnderlyingSymbol,
        QuoteDateTime,
        Root,
        Expiration,
        Strike,
        OptionType,
        Open,
        High,
        Low,
        Close,
        TradeVolume,
        BidSize,
        Bid,
        AskSize,
        Ask,
        UnderlyingBid,
        UnderlyingAsk,
        ImpliedUnderlyingPrice,
        ActiveUnderlyingPrice,
        ImpliedVolatility,
        Delta,
        Gamma,
        Theta,
        Vega,
        Rho, 
        OpenInterest
    }

    class Equity
    {
        internal DateTime dt;
        internal string symbol;
        internal float price;
    }

    enum PositionType
    {
        BSH,
        STTBWB,
        STTBWB_FSTT_45_30_15,
        STTBWB_FSTT_60_40_20,
        ProtectedJeep,
        PDS,
        PCS,
        CDS,
        CCS
    }

    // a Position represents a multi-legged option position that was opened at a starting date and time
    // The trades field contains the list of adjustments to the initial trade (trade[0]), incuding the final closing of the trade
    class Position
    {
        // currently held options with key of (root, expiration, strike, type); each (Option, int) is a reference to an Option, and the quantity in the position
        internal SortedList<(string, DateTime, int, LetsBeRational.OptionType), (Option, int)> options = new();

        internal PositionType positionType; // the original PositionType of the position
        internal List<Trade> trades = new List<Trade>(); // trades[0] contains the initial trade...so the Orders in that trade are the initial position
        internal DateTime entryDate; // so we can easily calculate DTE (days to expiration)
        internal float entryValue; // net price in dollars of all options in Position at entry
        internal float entryDelta; // net delta of all options in Position at entry

        internal float curValue; // internal state use by Backtest() to hold current value of Position in dollars
        internal float curDelta; // internal state used by backtest90 to hold current delta of position.
        internal bool closePosition; // internal state used by Backtest() function to decide if this position should be removed from this Position's option SortedList

        // add an option to this Position's options collection if it is not already there.
        // if it is already there, adjust quantity, and, if quantity now 0, remove it from this Position's option collection
        // returns false if option quantity became 0
        internal bool AddOption(Option option, int quantity)
        {
            Debug.Assert(quantity != 0);

            var key = (option.root, option.expiration, option.strike, option.optionType);
            (Option, int) value;
            int keyIndex = options.IndexOfKey(key);
            if (keyIndex >= 0)
            {
                value = options.Values[keyIndex];
                value.Item2 += quantity;
                if (value.Item2 == 0)
                {
                    options.RemoveAt(keyIndex);
                    return false;
                }
            }
            else
            {
                // option not in collection - add it
                options.Add((option.root, option.expiration, option.strike, option.optionType), (option, quantity));
            }

            return true; // option in collection (false meant it was in collection and now isn't because new quatity became 0)
        }

        internal virtual void adjust()
        {

        }
    }

    class BSH : Position
    {
        internal BSH()
        {
            positionType = PositionType.BSH;
        }

        internal override void adjust()
        {
            base.adjust();
        }
    }

    class STTBWB : Position
    {
        internal STTBWB()
        {
            positionType = PositionType.STTBWB;
        }

        internal override void adjust()
        {
            base.adjust();
        }
    }

    // a Trade is a list of filled Orders (1 Order for each different expiration/strik Put or Call in the Trade)
    class Trade
    {
        internal TradeType tradeType = TradeType.None;
        internal DateTime dt; // when orders placed and filled (this is a backtest...we assume orsers filled instantly)
        internal float commission; // total commission for all orders in Trade
        internal List<Order> orders = new List<Order>(); // each order is for a quantity of a single option
    }

    enum TradeType
    {
        None,
        BalancedButterfly,
        BrokenWingButterfly,
        PCS,
        PDS,
        Single
    }

    // an Order is for a filled quantity of Puts or Calls
    class Order
    {
        internal OrderType orderType = OrderType.None;
        internal OptionData option; // reference to option at entry
        internal int quantity;
    }

    enum OrderType
    {
        None,
        Put,
        Call,
        Stock
    }

#if false
    class OptionIndexes {
        Dictionary<ExpirationDate, Dictionary<int, Option>> expiration_strike_index; // for updating existing positions (strike and expiration are known); int is a strike, DateTime is an expiration
        SortedList<ExpirationDate, SortedList<int, Option>> expiration_delta_index; // for finding new positions by dte and delta (compute initial expiration from lowest dte)
        // sortedList = expirationRange_deltaRange_Index[expiration]; List<Option> lo = sortedList.Where(e => lowerDelta < e.Delta &&| e.Delta < higherDelta);
    }
#endif

    class Program
    {
        const bool noITMStrikes = true; // we're not interested in in the money strikes right now
        const int deepInTheMoneyAmount = 100; // # of SPX points at which we consider option "deep in the money"
        const int minStrike = 625;
        const int maxStrike = 10000;
        const int maxDTE = 200; // for reading data
        const int minDTEToOpen = 150; // for opening a position
        const int maxDTEToOpen = 170; // for opening a position
        const int minPositionDTE = 30;
        const float maxLoss = -2000f;
        const float profitTarget = 1000f;

        const string connectionString = "Host=localhost:5432;Username=postgres;Password=11331ca;Database=CBOEOptionData"; // Postgres
        const float Slippage = 0.05f; // from mid.. this should probably be dynamic based on current market conditions
        const float BaseCommission = 0.65f + 0.66f;
        const string DataDir = @"C:\Users\lel48\CBOEDataShop\SPX";
        const string expectedHeader = "underlying_symbol,quote_datetime,root,expiration,strike,option_type,open,high,low,close,trade_volume,bid_size,bid,ask_size,ask,underlying_bid,underlying_ask,implied_underlying_price,active_underlying_price,implied_volatility,delta,gamma,theta,vega,rho,open_interest";
        CultureInfo provider = CultureInfo.InvariantCulture;
        StreamWriter errorLog = new StreamWriter(Path.Combine(DataDir, "error_log.txt"));

        FredRateReader rate_reader = new FredRateReader(new DateTime(2013, 1, 1));
        SP500DividendYieldReader dividend_reader = new SP500DividendYieldReader(new DateTime(2013, 1, 1));

        List<Position> positions = new List<Position>();
        System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();

        // if SPX and SPXW exist for the same expiration date, we throw away the SPXW
        // For selecting new positions based on dte and strike: [Date][Time][Dte][Strike], or
        // For selecting new positions based on dte and delta: [Date][Time][Dte][Delta]; deltas are guaranteed to be unique and in order
        // StrikeIndex = SortedList<int, Option> is for updating existing positions given expiration date and strike
        // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
        // when we read data, we make sure that for puts, the delta of a smaller strike is less than the delta of a larger strike and,
        //  for calls, the delta of a smaller strike is greater than that of a larger strike
        // We separate this into a collect of days followed by a collection of times so we can read Day data in parallel
        SortedList<Day, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> PutOptions = new();
        SortedList<Day, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> CallOptions = new();

        static void Main(string[] args)
        {
            // test extensions to SortedList
            //TestSortedListExtensionClass.test();

            var pgm = new Program();
            pgm.run();
        }

        bool LogError(string error)
        {
            errorLog.WriteLine(error); 
            return false;
        }

        void run()
        {
#if false
            DateTime optdt = new DateTime(2019, 8, 7);
            DateTime expdt = new DateTime(2019, 8, 9);
            double price = (9.3 + 9.7) / 2.0;
            double d = 0.01 * dividend_reader.DividendYield(optdt); // trailing 12 - month sp500 dividend yield
            int dte = (expdt - optdt).Days;
            double t = dte / 365.0; // days to expiration / days in year
            double r = 0.01 * rate_reader.RiskFreeRate(optdt, dte); // risk free rate(1 year treasury yield)
            double s = (2888.04 + 2888.88) / 2; // underlying SPX price
            double K = 2850.0; // strike price
            double iv = LetsBeRational.ImpliedVolatility(price, s, K, t, r, d, LetsBeRational.OptionType.Put);
            double delta = LetsBeRational.Delta(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
            double theta = LetsBeRational.Theta(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
            double gamma = LetsBeRational.Gamma(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
            double vega = LetsBeRational.Vega(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
            double rho = LetsBeRational.Rho(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
#endif

            watch.Start();

            ReadDataAndComputeGreeks();
            errorLog.Close();

            watch.Stop();
            Console.WriteLine($"Time to read data and compute iv,delta: {0.001 * watch.ElapsedMilliseconds / 60.0} minutes");
            if (true) return;

            watch.Reset();
            watch.Start();

            Backtest();

            watch.Stop();
            Console.WriteLine($"Time to do backtest: {0.001 * watch.ElapsedMilliseconds / 60.0} minutes");

            Console.WriteLine("Hit any key to terminate");
            Console.ReadKey();
            int zzz = 1;
        }

        void ReadDataAndComputeGreeks()
        {
            // Dictionary<DateTime, float> RiskFreeRate = new Dictionary<DateTime, float>();
            // Dictionary<DateTime, float> SP500DivYield = new Dictionary<DateTime, float>();

#if false
            List<string> myList = new List<string>();
            IEnumerable<string> results = myList.Where(s => s == "abc");
            SortedList<int, Option> mySortList = new SortedList<int, Option>();
            IEnumerable<KeyValuePair<int, Option>> res = mySortList.Where(i => i.Key > 30 && i.Key < 60);
#endif

#if false
            // CBOEDataShop 15 minute data (900sec); a separate zip file for each day, so, if programmed correctly, we can read each day in parallel
            string[] zipFileNameArray = Directory.GetFiles(DataDir, "UnderlyingOptionsIntervals_900sec_calcs_oi*.zip", SearchOption.AllDirectories); // filename if you bought greeks
            //string[] zipFileNameArray = Directory.GetFiles(DataDir, "UnderlyingOptionsIntervalsQuotes_900sec*.zip", SearchOption.AllDirectories); // filename if you didn't buy greeks
            Array.Sort(zipFileNameArray);
#endif

#if false
            // first List is in order of Date; Second List is in order of time of day in fixed 15 minute increments
            // StrikeIndex = SortedList<int, Option> is for updateing existing positions given expiration date and strike
            // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
            List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> OptionData = new List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>>();
#endif

            // get first date
            DateTime first_dt, last_dt;
            string get_first_date_cmd = "select min(quotedatetime) from OptionData;";
            string get_last_date_cmd = "select max(quotedatetime) from OptionData;";
            using (NpgsqlConnection conn1 = new(connectionString))
            {
                conn1.Open();
                Npgsql.NpgsqlCommand sqlCommand1 = new(get_first_date_cmd, conn1); // Postgres
                first_dt = Convert.ToDateTime(sqlCommand1.ExecuteScalar());
                Npgsql.NpgsqlCommand sqlCommand2 = new(get_last_date_cmd, conn1); // Postgres
                last_dt = Convert.ToDateTime(sqlCommand2.ExecuteScalar());
            }

            // generate a list of weekdays (and exclude holidays)
            List<DateTime> dt_list = new() { first_dt };
            int numDays = (last_dt - first_dt).Days;
            TimeSpan one_day = new TimeSpan(1, 0, 0, 0);
            DateTime dt = first_dt;
            while (dt <= last_dt)
            {
                if (dt.DayOfWeek != DayOfWeek.Saturday && dt.DayOfWeek != DayOfWeek.Sunday)
                {
                    dt_list.Add(dt);
                }
                dt = dt.Add(one_day);
            }


            // initialize outer List (OptionData), which is ordered by Date, with new empty sub SortedList, sorted by time, for each date
            // since that sublist is the thing modified when a zip file is read, we can read in parallel without worrying about locks
            foreach (string zipFileName in zipFileNameArray)
            {
                DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));
                PutOptions.Add(zipDate, new SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
                CallOptions.Add(zipDate, new SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
            }

            // now read actual option data from each zip file (we have 1 zip file per day), row by row, and add it to SortedList for that date
#if PARFOR_READDATA
            Parallel.ForEach(zipFileNameArray, (zipFileName) =>
            {
#else
            foreach (string zipFileName in zipFileNameArray)
            {
#endif
                using (ZipArchive archive = ZipFile.OpenRead(zipFileName))
                {
                    Console.WriteLine($"Processing file: {zipFileName}");
                    string fileName = archive.Entries[0].Name;
                    if (archive.Entries.Count != 1)
                        Console.WriteLine($"Warning: {zipFileName} contains more than one file ({archive.Entries.Count}). Processing first one: {fileName}");
                    ZipArchiveEntry zip = archive.Entries[0];
                    DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));
                    SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> putOptionDataForDay = PutOptions[zipDate]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                    Debug.Assert(putOptionDataForDay.Count == 0);
                    SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> callOptionDataForDay = CallOptions[zipDate]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                    Debug.Assert(callOptionDataForDay.Count == 0);
                    Dictionary<ExpirationDate, List<OptionData>> expirationDictionary = new();
                    using (StreamReader reader = new StreamReader(zip.Open()))
                    {
                        bool validOption;
                        OptionData option = null;

                        // read header
                        string line;
                        try
                        {
                            line = reader.ReadLine();
                            if (line == null)
                                break;
                        }
                        catch (System.IO.InvalidDataException ex)
                        {
                            string errmsg = $"*Error* InvalidDataException reading file {zipFileName} Row 1 Message {ex.Message}";
                            Console.WriteLine(errmsg);
                            LogError(errmsg);
                            break;
                        }

                        if (!line.StartsWith(expectedHeader))
                        {
                            Console.WriteLine($"Warning: file {fileName} does not have expected header: {line}. Line skiped anyways");
                            Console.WriteLine($"         Expected header: {expectedHeader}");
                        }

                        int rowIndex = 1; // header was row 0, but will be row 1 if we look at data in Excel
                        int numValidOptions = 0;
                        while (true)
                        {
                            try
                            {
                                line = reader.ReadLine();
                                if (line == null)
                                    break;
                            }
                            catch (System.IO.InvalidDataException ex)
                            {
                                string errmsg = $"*Error* InvalidDataException reading file {zipFileName} Row {rowIndex + 1} Message {ex.Message}";
                                Console.WriteLine(errmsg);
                                LogError(errmsg);
                                break;
                            }
                            ++rowIndex;
                            option = new OptionData();
                            option.rowIndex = rowIndex;
                            validOption = ParseOption(noITMStrikes, maxDTEToOpen, line, option, zipDate, fileName, rowIndex);
                            if (validOption)
                            {
                                numValidOptions++;

                                // before creating collections for indexing, we have to make sure:
                                // 1. if there are SPX and SPXW/SPXQ options for the same expiration, we throw away the SPXW or SPXQ. If there are SPXW
                                //    and SPXQ options for the same expiration, we throw away the SPXQ 
                                // 2. If there are options with the same expiration but different strikes, but with the same delta, we adjust delta so that
                                //    if a call, the delta of the higher strike is strictly less than the delta of of a lower strike, and 
                                //    if a put, the delta of the higher strike is strictly greater than the delta of a lower strike.
                                //    We do this by minor adjustments to "true" delta
                                List<OptionData> optionList;
                                bool expirationFound = expirationDictionary.TryGetValue(option.expiration, out optionList);
                                if (!expirationFound)
                                {
                                    optionList = new List<OptionData>();
                                    optionList.Add(option);
                                    expirationDictionary.Add(option.expiration, optionList);
                                }
                                else
                                {
                                    OptionData optionInList = optionList.First();
                                    if (option.root == optionInList.root)
                                        optionList.Add(option);
                                    else
                                    {
                                        if (optionInList.root == "SPX")
                                            continue; // throw away new SPXW/SPXQ option that has same expiration as existing SPX option

                                        if (option.root == "SPX" || option.root == "SPXW") { 
                                            // throw away existing List and replace it with new list of options of root of new option
                                            optionList.Clear();
                                            optionList.Add(option);
                                        }
                                    }
                                }
                            }
                        }
                        int xxx = 1;
                    }

                    // now that we've thrown away SPXW options where there was an SPX option with the same expration, we start creating the main two
                    // indexes: StrikeIndex and DeltaIndex, which are both SortedList<int, OptionData>, for each time and expiration for this day.

                    // To start, we just create just the StrikeIndex and just add an empty DeltaIndex (SortedList<int, OptionData>)
                    // because of the possibility that two options with different strikes will actually have the same delta. Now...tis shouldn't be the
                    // case, but it might be in the data we read because way out of the money options have "funny" deltas sometimes. We will adjust the
                    // deltas that were read so the it's ALWAYS the case that farther out of the money options have lower deltas
                    foreach (var optionsListKVP in expirationDictionary)
                        foreach (OptionData option in optionsListKVP.Value)
                        {
                            if (option.optionType == LetsBeRational.OptionType.Put)
                                AddOptionToOptionDataForDay(option, putOptionDataForDay);
                            else
                                AddOptionToOptionDataForDay(option, callOptionDataForDay);
                        }

                    // now fill in unique deltas
#if PARFOR_READDATA
            });
#else
                }
#endif
                // now 
                int aa = 1;
            }
        }

        void AddOptionToOptionDataForDay(OptionData option, SortedList<Time, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay)
        {
            StrikeIndex optionDataForStrike;
            DeltaIndex optionDataForDelta;

            int indexOfOptionTime = optionDataForDay.IndexOfKey(option.dt);
            if (indexOfOptionTime == -1)
            {
                // first option of day - need to create SortedList for this time and add it to optionDataForDay
                optionDataForDay.Add(option.dt, new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>());
                indexOfOptionTime = optionDataForDay.IndexOfKey(option.dt);
            }

            // now create the two Index collections (one so we can iterate through strikes, the other so we can iterate through deltas)
            (StrikeIndex, DeltaIndex) optionDataForExpiration;
            var optionDataForTime = optionDataForDay.ElementAt(indexOfOptionTime).Value;

            bool expirationFound = optionDataForTime.TryGetValue(option.expiration, out optionDataForExpiration);
            if (!expirationFound)
            {
                optionDataForStrike = new StrikeIndex();
                optionDataForDelta = new DeltaIndex();
                optionDataForTime.Add(option.expiration, (optionDataForStrike, optionDataForDelta));
            }
            else
            {
                optionDataForStrike = optionDataForExpiration.Item1;
                Debug.Assert(optionDataForStrike != null);
                optionDataForDelta = optionDataForExpiration.Item2;
                Debug.Assert(optionDataForStrike != null);
                if (optionDataForStrike.ContainsKey(option.strike))
                {
                    Console.WriteLine($"Duplicate Strike at {option.dt}: expiration={option.expiration}, strike={option.strike}, ");
                    return;
                }
                while (optionDataForDelta.ContainsKey(option.delta100))
                {
                    //var xxx = optionDataForDelta[option.delta100]; // debug
                    if (option.optionType == LetsBeRational.OptionType.Put)
                        option.delta100--;
                    else
                        option.delta100++;
                }
            }
            optionDataForStrike.Add(option.strike, option);
            optionDataForDelta.Add(option.delta100, option);
        }

        bool ParseOption(bool noITMStrikes, int maxDTE, string line, OptionData option, DateTime zipDate, string fileName, int linenum)
        {
            Debug.Assert(option != null);

            string[] fields = line.Split(',');

            if (fields[0] != "^SPX")
                return LogError($"*Error*: underlying_symbol is not ^SPX for file {fileName}, line {linenum}, underlying_symbol {fields[0]}, {line}");

            option.root = fields[2].Trim().ToUpper();
            if (option.root != "SPX" && option.root != "SPXW" && option.root != "SPXQ")
            {
                if (option.root == "BSZ" || option.root == "SRO")
                    return false; // ignore binary options on SPX
                return LogError($"*Error*: root is not SPX, SPXW, or SPXQ for file {fileName}, line {linenum}, root {option.root}, {line}");
            }

            string optionType = fields[5].Trim().ToUpper();
            if (optionType != "P" && optionType != "C")
                return LogError($"*Error*: option_type is neither 'P' or 'C' for file {fileName}, line {linenum}, root {option.root}, {line}");
            option.optionType = (optionType == "P") ? LetsBeRational.OptionType.Put : LetsBeRational.OptionType.Call;

            //row.dt = DateTime.ParseExact(fields[1], "yyyy-MM-dd HH:mm:ss", provider);
            option.dt = DateTime.Parse(fields[(int)CBOEFields.DateTime]);
            Debug.Assert(option.dt.Date == zipDate); // you can have many, many options at same date/time (different strikes)

            //
            // temporarily not interested in option greeks before 10:00:00 and after 15:30:00
            //

            // not ever interested in options after 16:00:00
            switch (option.dt.Hour)
            {
                case 16:
                    if (option.dt.Minute > 0)
                        return false;
                    break;
            }

#if NO_CALLS
            // we're not interested in Calls right now
            if (option.optionType == LetsBeRational.OptionType.Call)
                return false;
#endif
            option.strike = (int)(float.Parse(fields[(int)CBOEFields.Strike]) + 0.001f); // +.001 to prevent conversion error
                                                                                         // for now, only conside strikes with even multiples of 25
#if ONLY25STRIKES
            if (option.strike % 25 != 0)
                return false;
#endif
            if (option.strike < minStrike || option.strike > maxStrike)
                return false;

            option.underlying = float.Parse(fields[(int)CBOEFields.UnderlyingBid]);
            if (option.underlying <= 0.0)
                return LogError($"*Error*: underlying_bid is 0 for file {fileName}, line {linenum}, {line}");
            if (option.underlying < 500.0)
                return LogError($"*Error*: underlying_bid is less than 500 for file {fileName}, line {linenum}, {line}");

            // we're not interested in ITM strikes right now
            if (noITMStrikes && option.strike >= option.underlying)
                return false;

            //row.expiration = DateTime.ParseExact(fields[3], "yyyy-mm-dd", provider);
            option.expiration = DateTime.Parse(fields[(int)CBOEFields.Expiration]);

            TimeSpan tsDte = option.expiration.Date - option.dt.Date;
            option.dte = tsDte.Days;
            if (option.dte < 0)
                return LogError($"*Error*: quote_datetime is later than expiration for file {fileName}, line {linenum}, {line}");

            // we're not interested in dte greater than 180 days
            if (option.dte > maxDTE)
                return false;

            option.bid = float.Parse(fields[(int)CBOEFields.Bid]);
            if (option.bid < 0f)
                return LogError($"*Error*: bid is less than 0 for file {fileName}, line {linenum}, bid {option.bid}, {line}");
            option.ask = float.Parse(fields[(int)CBOEFields.Ask]);
            if (option.ask < 0f)
                return LogError($"*Error*: ask is less than 0 for file {fileName}, line {linenum}, ask {option.ask}, {line}"); ;
            option.mid = (0.5f * (option.bid + option.ask));
#if true
            if (option.mid == 0)
            {
                option.iv = option.delta = option.gamma = option.vega = option.rho = 0f;
                return true; // I keep this option in case it is in a Position
            }
#endif
            // do my own computation if dte == 0 or iv == 0 or delta == 0
            option.iv = float.Parse(fields[(int)CBOEFields.ImpliedVolatility]);
            option.delta = float.Parse(fields[(int)CBOEFields.Delta]);

            if (option.dte == 0 || option.iv == 0 || option.delta == 0)
            {
                double dteFraction = option.dte;
                if (option.dte == 0)
                    dteFraction = (option.dt.TimeOfDay.TotalSeconds - 9*3600 + 1800) / (390*60); // fraction of 390 minute main session
                double t = dteFraction / 365.0; // days to expiration / days in year
                double s = option.underlying; // underlying SPX price
                double K = (double)option.strike; // strike price
                double q = 0.01 * dividend_reader.DividendYield(option.dt); // trailing 12 - month sp500 dividend yield
                double r = 0.01 * rate_reader.RiskFreeRate(option.dt, (option.dte == 0) ? 1 : option.dte); // risk free rate(1 year treasury yield)

                option.iv = (float)LetsBeRational.ImpliedVolatility((double)option.mid, s, K, t, r, q, LetsBeRational.OptionType.Put);
                option.delta = (float)LetsBeRational.Delta(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.delta100 = (int)(option.delta * 10000.0f);
                option.theta = (float)LetsBeRational.Theta(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.gamma = (float)LetsBeRational.Gamma(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.vega = (float)LetsBeRational.Vega(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.rho = (float)LetsBeRational.Rho(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                return true;
            }

            if (option.iv <= 0f)
                return LogError($"*Error*: implied_volatility is equal to 0 for file {fileName}, line {linenum}, iv {option.iv}, {line}"); ;
            if (option.delta == 0f)
                return LogError($"*Error*: delta is equal to 0 for file {fileName}, line {linenum}, {line}");
            if (Math.Abs(option.delta) == 1f)
                return LogError($"*Error*: absolute value of delta is equal to 1 for file {fileName}, line {linenum}, delta {option.delta}, {line}"); ;
            if (Math.Abs(option.delta) > 1f)
                return LogError($"*Error*: absolute value of delta is greater than 1 for file {fileName}, line {linenum}, delta {option.delta}, {line}"); ;
            option.delta100 = (int)(option.delta * 10000.0f);
            option.gamma = float.Parse(fields[(int)CBOEFields.Gamma]);
            option.theta = float.Parse(fields[(int)CBOEFields.Theta]);
            option.vega = float.Parse(fields[(int)CBOEFields.Vega]);
            option.rho = float.Parse(fields[(int)CBOEFields.Rho]);

            return true;
        }

        void ComputeGreeks(OptionData option)
        {
            // compute iv and delta of option
            double t = option.dte / 365.0;
            double r = 1.0; // 0.01*RateReader.RiskFreeRate(option.dt.Date, option.dte);
            double d = 2.0; // 0.01*DividendReader.DividendYield(option.dt.Date);
            option.riskFreeRate = (float)r;
            option.dividend = (float)d;

            // deep in the money options have iv=0, delta=1
            if (option.optionType == LetsBeRational.OptionType.Call)
            {
                if ((option.strike < ((int)option.underlying) - deepInTheMoneyAmount))
                {
                    option.iv = 0.0f;
                    option.delta100 = 10000;
                }
            }
            else if (option.strike > ((int)option.underlying + deepInTheMoneyAmount))
            {
                option.iv = 0.0f;
                option.delta100 = -10000;
            }
            else
            {
                option.iv = (float)LetsBeRational.ImpliedVolatility(option.mid, option.underlying, option.strike, t, r, d, option.optionType);
                if (Double.IsNaN(option.iv))
                {
                    int qq = 1;
                }
                double delta = LetsBeRational.Delta(option.underlying, option.strike, t, r, option.iv, d, option.optionType);
                if (Double.IsNaN(delta))
                {
                    int qq = 1;
                }
                double delta100f = 100.0 * delta;
                option.delta100 = (int)(10000.0 * delta);
                if (Math.Abs(option.delta100) > 10000)
                {
                    int cc = 1;
                }
                Debug.Assert(option.delta100 != -1);
                Debug.Assert(Math.Abs(option.delta100) <= 10000);
            }
            int a = 1;
        }

        void Backtest()
        {
            // a Position is a combination of Options that we are tracking as a single unit, like an STT-BWB, including adjustemnts
            // for each DateTime in DataList:
            // 1. update P&L, stats of existing Positions, and determine if existing Position needs to be adjusted or exited
            // 2. see if we can add a Position

            Console.WriteLine("");
            Console.WriteLine($"Starting backtest from {PutOptions.Keys[0].ToString("d")} to {PutOptions.Keys[PutOptions.Count - 1].ToString("d")}");

            // start at first date/time of data
            foreach (var keyValuePair in PutOptions)
            {
                DateTime day = keyValuePair.Key;
                SortedList<DateTime, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay = keyValuePair.Value;
                if (optionDataForDay.Count == 0)
                    Console.WriteLine($"No data for {day.ToString("d")}");
                else
                    Console.WriteLine($"Processing data for {day.ToString("d")}");

                foreach (var sortedListForTime in optionDataForDay)
                {
                    SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> optionDataForTime = sortedListForTime.Value;
                    Debug.Assert(optionDataForTime.Count > 0);
                    var i1 = optionDataForTime.Values[0].Item1; // StrikeIndex is a SortedList<int, optionData>
                    Debug.Assert(i1.Count > 0);
                    DateTime curDateTime = i1.Values[0].dt;
                    Console.WriteLine($"Testing at {curDateTime.ToString("HH.mm")}");

                    // loop through all exisiting positions, update their values, and see if they need to be adjusted or closed
#if PARFOR_ANALYZE
                    Parallel.ForEach(positions, (position) => {
#else
                    foreach (Position position in positions)
                    {
#endif
                        position.closePosition = false;

                        // compute value of position at current date/time
                        position.curValue = 0.0f;
                        position.curDelta = 0;

                        // new way
                        foreach (var kv in position.options)
                        {
                            var (option, quantity) = kv.Value;
                            OptionData curOption = option.optionData[curDateTime];
                            position.curValue += quantity * curOption.mid * option.multiplier; // a negative value means a credit
                            position.curDelta += quantity * curOption.delta100 * 0.01f;
                        }
#if false
                        /// old way
                        Debug.Assert(position.trades.Count > 0);
                        foreach (Trade trade in position.trades)
                        {
                            foreach (Order order in trade.orders)
                            {
                                Debug.Assert(order.option != null);
                                OptionData entry_option = order.option;
                                (StrikeIndex, DeltaIndex) optionsForExpirationDate = optionDataForTime[entry_option.expiration];
                                OptionData curOption = optionsForExpirationDate.Item1[entry_option.strike];
                                position.curValue += order.quantity * curOption.mid * 100.0f; // a negative value means a credit
                                position.curDelta += order.quantity * curOption.delta100*0.01f;
                            }
                        }
#endif
                        // see if we need to close or adjust position
                        // first check the things that all option positions must check: maxLoss, profitTarget, minPositionDTE
                        float pl = position.curValue - position.entryValue;
                        if (pl < maxLoss)
                        {
                            position.closePosition = true;
                        }
                        else if (position.curValue >= profitTarget)
                        {
                            position.closePosition = true;
                        }
                        else if ((day.Date - position.entryDate.Date).Days < minPositionDTE)
                        {
                            // close trade if dte of original trade is too small
                            position.closePosition = true;
                        }
                        else
                        {
                            // see if we need to adjust or close position
                            position.adjust();
                        }
#if PARFOR_ANALYZE
                    });
#else
                    }
#endif
                    // now remove any closed positions from positions collection
                    positions.RemoveAll(item => item.closePosition);

                    // now select new positions for this date and time
                    // first, just select expirations with 120 to 150 dte
                    DateTime initialExpirationDate = day.AddDays(minDTEToOpen);
                    DateTime finalExpirationDate = day.AddDays(maxDTEToOpen);

                    int startIndex = optionDataForTime.IndexOfFirstDateGreaterThanOrEqualTo(initialExpirationDate);
                    int endIndex = optionDataForTime.IndexOfFirstDateLessThanOrEqualTo(finalExpirationDate);
                    if (startIndex >= 0)
                    {
                        if (endIndex < 0)
                        {
                            endIndex = optionDataForTime.Count - 1;
                        }
                        for (int i = startIndex; i <= endIndex; i++)
                        {
                            (StrikeIndex, DeltaIndex) indexPair = optionDataForTime.ElementAt(i).Value;
                            if (indexPair.Item1 == null)
                            {
                                Debug.Assert(indexPair.Item2 == null);
                                continue;
                            }
                            if (indexPair.Item2 == null)
                                continue;

                            // DeltaIndex is a SortedList<int, Option)
                            // each element of deltaList is a <Key, Value> pair, with the key being the delta and the value being the option
                            DeltaIndex strikeByDeltaList = indexPair.Item2;
                            FindSTTBWBs(strikeByDeltaList);
                        }
                    }
                }
            }
        }

        // For now we try and select an STT at 25-15-5
        void FindSTTBWBs(DeltaIndex strikeByDeltaList)
        {

            // DeltaIndex is a SortedList<int, Option)
            // each element of deltaList is a <Key, Value> pair, with the key being the delta and the value being the option

            // find delta of -4  to -6
            var deltaList5 = strikeByDeltaList.Where(e => e.Key <= -400 && e.Key >= -600);
            Int32 deltaList5Count = deltaList5.Count();
            if (deltaList5.Count() == 0)
                return;

            // find delta of -13 to -16
            var deltaList15 = strikeByDeltaList.Where(e => e.Key <= -1300 && e.Key >= -1600);
            Int32 deltaList15Count = deltaList15.Count();
            if (deltaList15.Count() == 0)
                return;

            // find delta of -23 to -26
            var deltaList25 = strikeByDeltaList.Where(e => e.Key <= -2300 && e.Key >= -2600);
            Int32 deltaList25Count = deltaList25.Count();
            if (deltaList25.Count() == 0)
                return;

            foreach (var delta25kv in deltaList25)
            {
                OptionData opt25 = delta25kv.Value;
                int opt25Delta = opt25.delta100;
                foreach (var delta15kv in deltaList15)
                {
                    OptionData opt15 = delta15kv.Value;
                    int opt15Delta = opt15.delta100;
                    foreach (var delta5kv in deltaList5)
                    {
                        OptionData opt5 = delta5kv.Value;
                        int opt5Delta = opt5.delta100;

                        // calculate total delta, cost
                        float totalDelta = (4 * opt25Delta - 8 * opt15Delta + 4 * opt5Delta) * .01f;
                        //float totalCostNoSlippage = (-4 * opt25.mid + 8 * opt15.mid - 4 * opt5.mid) * 100.0f;

                        // only create Trade if delta between -0.5 and 0.5
                        if (totalDelta <= 0.5 && totalDelta >= -0.5)
                        {
                            Position position = AddSTTBWB(opt5, opt15, opt25);
                            int yqrs = 1;
                        }
                    }
                }
            }
        }

        Position AddSTTBWB(OptionData opt5, OptionData opt15, OptionData opt25)
        {
            float totalDelta = (4 * opt25.delta100 - 8 * opt15.delta100 + 4 * opt5.delta100) * .01f;
            float value = (4 * opt25.mid - 8 * opt15.mid + 4 * opt5.mid) * 100f; // a negative value means a credit
            float commission = 16f * BaseCommission;
            float slippage = 16 * Slippage * 100.0f;
            float costs = slippage + commission;

            Position position = new Position();
            position.positionType = PositionType.STTBWB;
            position.entryDelta = position.curDelta = totalDelta;
            position.entryValue = position.curValue = value;

            Trade trade = new Trade();
            trade.tradeType = TradeType.BrokenWingButterfly;
            trade.dt = opt5.dt;
            trade.orders = new List<Order>();
            trade.commission = commission;

            Order delta5Order = new Order();
            delta5Order.orderType = OrderType.Put;
            delta5Order.quantity = 4;
            delta5Order.option = opt5;
            trade.orders.Add(delta5Order);

            Order delta15Order = new Order();
            delta15Order.orderType = OrderType.Put;
            delta15Order.quantity = -8;
            delta15Order.option = opt15;
            trade.orders.Add(delta15Order);

            Order delta25Order = new Order();
            delta25Order.orderType = OrderType.Put;
            delta25Order.quantity = 4;
            delta25Order.option = opt25;

            trade.orders.Add(delta25Order);
            position.trades.Add(trade);
            position.entryDate = opt25.dt;
            positions.Add(position);

            return position;
        }
    }

    public class FloatInterval
    {
        private float start, end;

        public FloatInterval(float start, float end)
        {
            this.start = start;
            this.end = end;
        }

        public bool InOpenInterval(float value)
        {
            return (value > start) && (value < end);
        }

        public bool InClosedInterval(float value)
        {
            return (value >= start) && (value <= end);
        }

        public bool InLeftClosedInterval(float value)
        {
            return (value >= start) && (value < end);
        }

        public bool InRightClosedInterval(float value)
        {
            return (value > start) && (value <= end);
        }
    }
}

namespace SortedListExtensions
{
    using OptionBacktester;
    using StrikeIndex = SortedList<int, OptionBacktester.OptionData>;
    using DeltaIndex = SortedList<int, OptionBacktester.OptionData>;
    using ExpirationDate = DateTime;

    public static class TestSortedListExtensionClass
    {
        internal static void test()
        {
            var strikeIndex = new StrikeIndex();
            var deltaIndex = new DeltaIndex();
            var list = new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>();

            DateTime d1 = new DateTime(2021, 1, 1);
            list.Add(d1, (strikeIndex, deltaIndex));
            var i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);
            i1 = list.IndexOfFirstDateLessThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);

            DateTime d2 = new DateTime(2021, 1, 2);
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d2);
            Debug.Assert(i1 == -1);
            DateTime d0 = new DateTime(2020, 1, 1);
            i1 = list.IndexOfFirstDateLessThanOrEqualTo(d0);
            Debug.Assert(i1 == 0);

            list.Add(d2, (strikeIndex, deltaIndex));
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d2);
            Debug.Assert(i1 == 1);
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);

            int xx = 1;
        }
    }

    public static class SortedListExtensionClass
    {
        internal static int IndexOfFirstDateGreaterThanOrEqualTo(this SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> options, ExpirationDate expirationDate)
        {
            Debug.Assert(options.Count > 0);
            int minIdx = 0;
            int midIdx;
            int maxIdx = options.Count - 1;

            if (maxIdx == 0)
                return (options.First().Key >= expirationDate) ? 0 : -1;

            while (minIdx < maxIdx)
            {
                midIdx = (minIdx + maxIdx) / 2;
                DateTime dt = options.ElementAt(midIdx).Key;
                if (dt == expirationDate)
                {
                    return midIdx;
                }
                if (dt < expirationDate)
                {
                    minIdx = midIdx + 1;
                }
                else // dt > expirationDate
                {
                    maxIdx = midIdx;
                }
            }
            return minIdx;
        }

        internal static int IndexOfFirstDateLessThanOrEqualTo(this SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> options, ExpirationDate expirationDate)
        {
            Debug.Assert(options.Count > 0);
            int minIdx = 0;
            int midIdx;
            int maxIdx = options.Count - 1;

            if (maxIdx == 0)
                return (options.First().Key <= expirationDate) ? 0 : -1;

            while (minIdx < maxIdx)
            {
                midIdx = (minIdx + maxIdx) / 2;
                DateTime dt = options.ElementAt(midIdx).Key;
                if (dt == expirationDate)
                {
                    return midIdx;
                }
                if (dt > expirationDate)
                {
                    maxIdx = midIdx - 1;
                }
                else // dt < expirationDate
                {
                    minIdx = midIdx;
                }
            }
            Debug.Assert(minIdx == maxIdx);
            return minIdx;
        }
    }
}


