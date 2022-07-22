// This program backtest complex optionData positions
// It uses optionData price data from CBOE Datashop
// it gets SP500 dividend yield data from Quandl
// It gets Risk Free Interest rates from FRED
// It uses my modified version of Jaeckel's Lets Be Rational C++ program to compute optionData greeks

// This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis

#define NO_CALLS
#define ONLY25STRIKES
#undef PARFOR_READDATA
#undef PARFOR_ANALYZE

using System;
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Npgsql; // for postgres

namespace OptionBacktester
{
    using StrikeIndex = SortedList<int, OptionData>; // index is strike

    // delta index is delta*deltaIndexMultiplier; since delta ca be -1 to 1, deltaIndex can be -10000 to 10000
    // for a deltaMultiplier of 10000, delta of -0.05 for a put has a delta index of -.05*deltaIndexMultiplier = -500
    using DeltaIndex = SortedList<int, OptionData>;

    using ExpirationDate = DateOnly;
    using SortedListExtensions;
    using System.Collections.Concurrent;
    using System.Threading;

    class Option
    {
        internal string root;
        internal DateOnly expiration;
        internal int strike;
        internal OptionType optionType;
        internal float multiplier = 100f; // converts optionData prices to dollars
        internal SortedList<DateTime, OptionData> optionDataList = new ();

        internal Option(OptionData optionData)
        {
            root = optionData.root;
            expiration = optionData.expiration;
            strike = optionData.strike;
            optionType = optionData.optionType;
            optionDataList.Add(optionData.dt, optionData);

            optionData.option = this;
        }
    }

    public enum OptionType
    {
        Put,
        Call
    }

    class OptionData
    {
        internal const float deltaIndexMultiplier = 10000.0f;

        internal Option option; // back pointer into Options collection

        internal DateTime dt;
        internal DateOnly expiration;
        internal int strike;
        internal OptionType optionType;
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
        internal float mid;
        internal int dte;

        // deltaIndex is delta*deltaIndexMultiplier cast to an int so it makes a good index;
        // so, if delta is read as -0.5 (at the money put), and deltaIndexMultiplier is 10000, it will have a deltaIndex of -5000
        internal int deltaIndex = (int)(-1 * deltaIndexMultiplier); // a delta of -1 becomes a deltaIndex of -10000
    }

    // for reading CBOE Data
    public enum CBOEFields : int
    {
        UnderlyingSymbol = 0,
        QuoteDateTime,
        Expiration,
        Strike,
        OptionType,
        Root,
        Underlying,
        Bid,
        Ask,
        ImpliedVolatility,
        Delta,
        Gamma,
        Theta,
        Vega,
        Rho,
        OpenInterest,
        RiskFreeRate,
        DividendYield
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

    // a Position represents a multi-legged optionData position that was opened at a starting date and time
    // The trades field contains the list of adjustments to the initial trade (trade[0]), incuding the final closing of the trade
    class Position : IEqualityComparer<Position>
    {
        internal static int positionId = 0; // only increment with InterlockedIncrement(ref positionKey)
        // currently held options with key of (root, expiration, strike, type); each (Option, int) is a reference to an Option, and the quantity in the position
        internal SortedList<(string root, ExpirationDate expiration, int strike, OptionType type), (Option option, int quantity)> options = new();

        internal PositionType positionType; // the original PositionType of the position
        internal List<Trade> trades = new List<Trade>(); // trades[0] contains the initial trade...so the Orders in that trade are the initial position
        internal DateTime entryDateTime; // so we can easily calculate DTE (days to expiration)
        internal float entryValue; // net price in dollars of all options in Position at entry
        internal float entryDelta; // net delta of all options in Position at entry

        internal DateTime curDateTime; // so we can easily calculate final value of position
        internal float curValue; // internal state use by Backtest() to hold current value of Position in dollars
        internal float curDelta; // internal state used by backtest() to hold current delta of position.

        // add an Option to this Position's options collection if it is not already there.
        // if it is already there, adjust quantity, and, if quantity now 0, remove it from this Position's optionData collection
        // returns false if optionData quantity became 0
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
                // Option not in collection - add it
                options.Add(key, (option, quantity));
            }

            return true; // optionData in collection (false meant it was in collection and now isn't because new quatity became 0)
        }

        internal virtual void adjust()
        {

        }

        public bool Equals(Position x, Position y) => true;

        public int GetHashCode(Position position) => position?.entryDateTime.GetHashCode() ?? throw new ArgumentNullException(nameof(position));
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
        internal List<Order> orders = new List<Order>(); // each order is for a quantity of a single optionData
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
        internal OptionData option; // reference to optionData at entry
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
        Dictionary<ExpirationDate, Dictionary<int, Option>> expiration_strike_index; // for updating existing positions (strike and expiration are known); int is a strike
        SortedList<ExpirationDate, SortedList<int, Option>> expiration_delta_index; // for finding new positions by dte and delta (compute initial expiration from lowest dte)
        // sortedList = expirationRange_deltaRange_Index[expiration]; List<Option> lo = sortedList.Where(e => lowerDelta < e.Delta &&| e.Delta < higherDelta);
    }
#endif

    class Program
    {
        const int deepInTheMoneyAmount = 100; // # of SPX points at which we consider optionData "deep in the money"
        const int minStrike = 625;
        const int maxStrike = 10000;
        const int maxDTE = 200; // for reading data
        const int minDTEToOpen = 150; // for opening a position
        const int maxDTEToOpen = 170; // for opening a position
        const int maxDIT = 70;
        const float maxLoss = -2000f;
        const float profitTarget = 1000f;

        // these are delta*deltaIndexMultiplier (used to convert float delta into int delta index)
        const int delta5_minIndex = (int)(.04 * OptionData.deltaIndexMultiplier);
        const int delta5_maxIndex = (int)(.06 * OptionData.deltaIndexMultiplier);
        const int delta15_minIndex = (int)(.14 * OptionData.deltaIndexMultiplier);
        const int delta15_maxIndex = (int)(.16 * OptionData.deltaIndexMultiplier);
        const int delta25_minIndex = (int)(.24 * OptionData.deltaIndexMultiplier);
        const int delta25_maxIndex = (int)(.26 * OptionData.deltaIndexMultiplier);

        const string connectionString = "Host=localhost:5432;Username=postgres;Password=11331ca;Database=CBOEOptionData"; // Postgres
        const float Slippage = 0.05f; // from mid.. this should probably be dynamic based on current market conditions
        const float BaseCommission = 0.65f + 0.66f;
        StreamWriter errorLog = new StreamWriter(@"C:\Users\lel48\VisualStudioProjects\OptionBacktester.cs\error_log.txt");

        ConcurrentDictionary<int, Position> positions = new(); // concurrent because we analyze each position asynchronously
        ConcurrentDictionary<int, Position> closedPositions = new(); // concurrent because we add Positions removed from positions asynchronously

        System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();

        // if SPX and SPXW exist for the same expiration date, we throw away the SPXW
        // For selecting new positions based on dte and strike: [Date][Time][Dte][Strike], or
        // For selecting new positions based on dte and delta: [Date][Time][Dte][Delta]; deltas are guaranteed to be unique and in order
        // StrikeIndex = SortedList<int, Option> is for updating existing positions given expiration date and strike
        // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
        // when we read data, we make sure that for puts, the delta of a smaller strike is less than the delta of a larger strike and,
        //  for calls, the delta of a smaller strike is greater than that of a larger strike
        // We separate this into a collect of days followed by a collection of times so we can read Day data in parallel
        Dictionary<(OptionType, string root, ExpirationDate, int strike), Option> options = new();
        SortedList<DateOnly, SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> PutOptions = new();
        SortedList<DateOnly, SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> CallOptions = new();

        static void Main(string[] args)
        {
            // test extensions to SortedList
            // TestSortedListExtensionClass.test();
            //int[] a = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12};
            //int idx = find_first_ge(a, 12);

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
#if false // for debugging
            DateOnly optdt = new DateOnly(2019, 8, 7);
            DateOnly expdt = new DateOnly(2019, 8, 9);
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
            //if (true) return;

            watch.Reset();
            watch.Start();

            CheckOptionData();

            Backtest();

            watch.Stop();
            Console.WriteLine($"Time to do backtest: {0.001 * watch.ElapsedMilliseconds / 60.0} minutes");

            Console.WriteLine("Hit any key to terminate");
            Console.ReadKey();
            int zzz = 1;
        }

        void ReadDataAndComputeGreeks()
        {
            // Dictionary<DateOnly, float> RiskFreeRate = new Dictionary<DateOnly, float>();
            // Dictionary<DateOnly, float> SP500DivYield = new Dictionary<DateOnly, float>();

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
            using (NpgsqlConnection conn = new(connectionString))
            {
                conn.Open();
                Npgsql.NpgsqlCommand sqlCommand1 = new(get_first_date_cmd, conn); // Postgres
                first_dt = Convert.ToDateTime(sqlCommand1.ExecuteScalar());
                Npgsql.NpgsqlCommand sqlCommand2 = new(get_last_date_cmd, conn); // Postgres
                last_dt = Convert.ToDateTime(sqlCommand2.ExecuteScalar());
            }

            // generate a list of weekdays (and exclude holidays)
            List<DateOnly> date_list = new();
            int numDays = (last_dt - first_dt).Days;
            DateOnly last_date = DateOnly.FromDateTime(last_dt);
            DateOnly date = DateOnly.FromDateTime(first_dt);
            while (date <= last_date)
            {
                if (date.DayOfWeek != DayOfWeek.Saturday && date.DayOfWeek != DayOfWeek.Sunday)
                {
                    date_list.Add(date);
                }
                date = date.AddDays(1);
            }

            // initialize outer List (OptionData), which is ordered by Expiration Date, with new empty sub SortedList, sorted by time, for each date
            // since that sublist is the thing modified when a zip file is read, we can read in parallel without worrying about locks
            foreach (DateOnly quote_date in date_list)
            {
                PutOptions.Add(quote_date, new SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
                CallOptions.Add(quote_date, new SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>());
            }
#if false
            // pre-compile man data query
            using (NpgsqlConnection conn = new(connectionString))
            {
                conn.Open();
                const string prepareStmt = "PREPARE get_quotes_query(timestamp, timestamp) AS select * from OptionData where optiontype = 'P' and quotedatetime >= $1 and  quotedatetime <= $2;";
                Npgsql.NpgsqlCommand sqlPrepareCommand = new(prepareStmt, conn);
                sqlPrepareCommand.ExecuteNonQuery();
            }
#endif
            // now read actual optionData data from postgres, and add it to SortedList for that date
#if PARFOR_READDATA
            Parallel.ForEach(date_list, new ParallelOptions { MaxDegreeOfParallelism = 16 }, (quote_date) =>
            {
#else
            foreach (DateOnly quote_date in date_list)
            {
#endif
                Console.WriteLine($"Reading date: {quote_date.ToString("yyyy-MM-dd")}");
                SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> putOptionDataForDay = PutOptions[quote_date]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                Debug.Assert(putOptionDataForDay.Count == 0);
                SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> callOptionDataForDay = CallOptions[quote_date]; // optionDataForDay is 3d List[time][expiration][(strike,delta)]
                Debug.Assert(callOptionDataForDay.Count == 0);
                Dictionary<ExpirationDate, List<OptionData>> expirationDictionary = new();

                using NpgsqlConnection conn = new(connectionString);
                conn.Open();
#if NO_CALLS
                //string get_quotes_from_day = $"select * from OptionData where optiontype = 'P' and quotedatetime >= '{quote_date.ToString("yyyy-MM-dd 00:00:00")}' and  quotedatetime <= '{quote_date.ToString("yyyy-MM-dd 23:59:59")}';";
                string get_quotes_from_day = $"select * from OptionData where optiontype = 'P' and quotedatetime >= '{quote_date.ToString("yyyy-MM-dd")} 00:00:00' and quotedatetime <= '{quote_date.ToString("yyyy-MM-dd")} 23:59:59';";
#else
                string get_quotes_from_day = $"select * from OptionData where quotedatetime >= '{quote_date.ToString("yyyy-MM-dd 00:00:00")}' and  quotedatetime <= '{quote_date.ToString("yyyy-MM-dd 23:59:59")}';";
#endif

#if false
                // execute pre-compiled data query with parameters for this day
                string executeStmt = $"EXECUTE get_quotes_query('{quote_date.ToString("yyyy-MM-dd 00:00:00")}', '{quote_date.ToString("yyyy-MM-dd 23:59:59")}');";
                Npgsql.NpgsqlCommand sqlCommand = new(executeStmt, conn); // Postgres
#endif
                Npgsql.NpgsqlCommand sqlCommand = new(get_quotes_from_day, conn); // Postgres
                NpgsqlDataReader reader = sqlCommand.ExecuteReader();

                bool validOption;
                int rowIndex = 1; // header was row 0, but will be row 1 if we look at data in Excel
                int numValidOptions = 0;
                while (reader.Read())
                {
                    ++rowIndex;

                    validOption = ParseOption(maxDTEToOpen, reader, out OptionData optionData, rowIndex);
                    if (!validOption)
                        continue;
                    numValidOptions++;

                    // before creating collections for indexing, we have to make sure:
                    // 1. if there are SPX and SPXW/SPXQ options for the same expiration, we throw away the SPXW or SPXQ. If there are SPXW
                    //    and SPXQ options for the same expiration, we throw away the SPXQ
                    //    But...here's the problem. If we open a position in SPXQ, we might end up throwing away later values of that option.
                    //    This happened apparently with SPXQ 100 Put exp 9/30/2014, where we saved data only through 7/2/2014, and threw it away
                    //    Afterwards
                    // 2. If there are options with the same expiration but different strikes, but with the same delta, we adjust delta so that
                    //    if a call, the delta of the higher strike is strictly less than the delta of of a lower strike, and 
                    //    if a put, the delta of the higher strike is strictly greater than the delta of a lower strike.
                    //    We do this by minor adjustments to "true" delta
                    List<OptionData> optionList;
                    bool expirationFound = expirationDictionary.TryGetValue(optionData.expiration, out optionList);
                    if (!expirationFound)
                    {
                        optionList = new List<OptionData>();
                        optionList.Add(optionData);
                        expirationDictionary.Add(optionData.expiration, optionList);
                    }
                    else
                    {
                        OptionData optionInList = optionList.First();
                        if (optionData.root == optionInList.root)
                        {
                            optionList.Add(optionData);
#if false
                            // debug possible issue of throwing away some of SPXQ data after a certain date time
                            if (optionData.root == "SPXQ")
                            {
                                if ((optionData.optionType == OptionType.Put) && (optionData.strike == 1100) && (optionData.expiration == new DateOnly(2014, 9, 30)))
                                {
                                    if (DateOnly.FromDateTime(optionData.dt) == new DateOnly(2014, 7, 2))
                                    {
                                        int abc = 1;
                                    }
                                }
                            }
#endif
                        }
                        else
                        {
                            if (optionInList.root == "SPX")
                            {
#if false
                                // debug possible issue of throwing away some of SPXQ data after a certain date time
                                if (optionData.root == "SPXQ")
                                {
                                    if ((optionData.optionType == OptionType.Put) && (optionData.strike == 1100) && (optionData.expiration == new DateOnly(2014, 9, 30)))
                                    {
                                        if (DateOnly.FromDateTime(optionData.dt) == new DateOnly(2014, 7, 3))
                                        {
                                            int abc = 1;
                                        }
                                    }
                                }
#endif
                                continue; // throw away new SPXW/SPXQ optionData that has same expiration as existing SPX optionData
                            }

                            if (optionData.root == "SPX" || optionData.root == "SPXW")
                            {
                                // throw away existing List and replace it with new list of options of root of new optionData
                                optionList.Clear();
                                optionList.Add(optionData);
                            }
                        }
                    }

                    // now add option part of optionData to main options dictionary if not already there
                    // if it is already there, just add optionData to Options's optionDataList
                    var key = (optionData.optionType, optionData.root, optionData.expiration, optionData.strike);
                    bool found = options.TryGetValue(key, out Option option);
                    if (!found)
                        options.Add(key, new Option(optionData)); // constructor adds optionData to new Option's optionDataList
                    else
                    {
                        optionData.option = option;
                        option.optionDataList.Add(optionData.dt, optionData);
                    }
                }

                // now that we've thrown away SPXW options where there was an SPX optionData with the same expration, we start creating the main two
                // indexes: StrikeIndex and DeltaIndex, which are both SortedList<int, OptionData>, for each time and expiration for this day.

                // To start, we just create just the StrikeIndex and just add an empty DeltaIndex (SortedList<int, OptionData>)
                // because of the possibility that two options with different strikes will actually have the same delta. Now...tis shouldn't be the
                // case, but it might be in the data we read because way out of the money options have "funny" deltas sometimes. We will adjust the
                // deltas that were read so the it's ALWAYS the case that farther out of the money options have lower deltas
                foreach (var optionsListKVP in expirationDictionary)
                {
                    foreach (OptionData optionData in optionsListKVP.Value)
                    {
                        if (optionData.optionType == OptionType.Put)
                            AddOptionDataToOptionDataForDay(optionData, putOptionDataForDay);
                        else
                            AddOptionDataToOptionDataForDay(optionData, callOptionDataForDay);
                    }
                }
#if PARFOR_READDATA
            });
#endif
                // now 
                int aa = 1;
            }
            int bb = 1;
        }

        void AddOptionDataToOptionDataForDay(OptionData optionData, SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay)
        {
            StrikeIndex optionDataForStrike;
            DeltaIndex optionDataForDelta;

            TimeOnly quote_time = TimeOnly.FromDateTime(optionData.dt);
            int indexOfOptionTime = optionDataForDay.IndexOfKey(quote_time);
            if (indexOfOptionTime == -1)
            {
                // first optionData of day - need to create SortedList for this time and add it to optionDataForDay
                optionDataForDay.Add(quote_time, new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>());
                indexOfOptionTime = optionDataForDay.IndexOfKey(quote_time);
            }

            // now create the two Index collections (one so we can iterate through strikes, the other so we can iterate through deltas)
            (StrikeIndex, DeltaIndex) optionDataForExpiration;
            var optionDataForTime = optionDataForDay.ElementAt(indexOfOptionTime).Value;

            bool expirationFound = optionDataForTime.TryGetValue(optionData.expiration, out optionDataForExpiration);
            if (!expirationFound)
            {
                optionDataForStrike = new StrikeIndex();
                optionDataForDelta = new DeltaIndex();
                optionDataForTime.Add(optionData.expiration, (optionDataForStrike, optionDataForDelta));
            }
            else
            {
                optionDataForStrike = optionDataForExpiration.Item1;
                Debug.Assert(optionDataForStrike != null);
                optionDataForDelta = optionDataForExpiration.Item2;
                Debug.Assert(optionDataForStrike != null);
                if (optionDataForStrike.ContainsKey(optionData.strike))
                {
                    Console.WriteLine($"Duplicate Strike at {optionData.dt}: expiration={optionData.expiration}, strike={optionData.strike}, ");
                    return;
                }
                while (optionDataForDelta.ContainsKey(optionData.deltaIndex))
                {
                    if (optionData.optionType == OptionType.Put)
                        optionData.deltaIndex--;
                    else
                        optionData.deltaIndex++;
                }
            }
            optionDataForStrike.Add(optionData.strike, optionData);
            optionDataForDelta.Add(optionData.deltaIndex, optionData);
        }

        bool ParseOption(int maxDTE, NpgsqlDataReader reader, out OptionData option, int linenum)
        {
            option = new();

            char optionType = reader.GetChar((int)CBOEFields.OptionType); // TODO: make sure database loader does .Trim().ToUpper();
            option.optionType = (optionType == 'P') ? OptionType.Put : OptionType.Call;

            option.root = reader.GetString((int)CBOEFields.Root).TrimEnd();  // TODO: make sure database loader does Trim().ToUpper();
            Debug.Assert(option.root == "SPX" || option.root == "SPXW" || option.root == "SPXQ");

            option.dt = reader.GetDateTime((int)CBOEFields.QuoteDateTime);

            option.strike = reader.GetInt32((int)CBOEFields.Strike); // +.001 to prevent conversion error
                                                                     // for now, only conside strikes with even multiples of 25
#if ONLY25STRIKES
            if (option.strike % 25 != 0)
                return false;
#endif
            if (option.strike < minStrike || option.strike > maxStrike)
                return false;

            option.underlying = reader.GetFloat((int)CBOEFields.Underlying); // TODO: check that database loadr uses activeUndrlyting
            Debug.Assert(option.underlying > 500f && option.underlying < 100000f);

            option.expiration = DateOnly.FromDateTime(reader.GetDateTime((int)CBOEFields.Expiration));

            //TimeSpan tsDte = optionData.expiration - DateOnly.FromDateTime(optionData.dt);
            option.dte = option.expiration.DayNumber - DateOnly.FromDateTime(option.dt).DayNumber;
            Debug.Assert(option.dte >= 0);

            // we're not interested in dte greater than 180 days
            if (option.dte > maxDTE)
                return false;

            option.bid = reader.GetFloat((int)CBOEFields.Bid);
            Debug.Assert(option.bid >= 0f);
            option.ask = reader.GetFloat((int)CBOEFields.Ask);
            Debug.Assert(option.ask >= 0f);
            option.mid = (0.5f * (option.bid + option.ask));
#if false // check that database loader does this
            if (option.mid == 0)
            {
                option.iv = option.delta = option.gamma = option.vega = option.rho = 0f;
                return true; // I keep this option in case it is in a Position
            }
#endif
            option.iv = reader.GetFloat((int)CBOEFields.ImpliedVolatility);
            option.delta = reader.GetFloat((int)CBOEFields.Delta);
            option.deltaIndex = (int)(option.delta * 100000.0);
            option.theta = reader.GetFloat((int)CBOEFields.Theta);
            option.gamma = reader.GetFloat((int)CBOEFields.Gamma);
            option.vega = reader.GetFloat((int)CBOEFields.Vega);
#if false  // check that database loader does this
            if (option.dte == 0 || option.iv == 0 || option.delta == 0)
            {
                double dteFraction = option.dte;
                if (option.dte == 0)
                    dteFraction = (option.dt.TimeOfDay.TotalSeconds - 9 * 3600 + 1800) / (390 * 60); // fraction of 390 minute main session
                double t = dteFraction / 365.0; // days to expiration / days in year
                double s = option.underlying; // underlying SPX price
                double K = (double)option.strike; // strike price
                double q = 0.01 * dividend_reader.DividendYield(option.dt); // trailing 12 - month sp500 dividend yield
                double r = 0.01 * rate_reader.RiskFreeRate(option.dt, (option.dte == 0) ? 1 : option.dte); // risk free rate(1 year treasury yield)

                option.iv = (float)LetsBeRational.ImpliedVolatility((double)option.mid, s, K, t, r, q, LetsBeRational.OptionType.Put);
                option.delta = (float)LetsBeRational.Delta(s, K, t, r, option.iv, q, LetsBeRational.OptionType.Put);
                option.deltaIndex = (int)(option.delta * deltaIndexMultiplier);
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
            option.deltaIndex = (int)(option.delta * deltaIndexMultiplier);
            option.gamma = float.Parse(fields[(int)CBOEFields.Gamma]);
            option.theta = float.Parse(fields[(int)CBOEFields.Theta]);
            option.vega = float.Parse(fields[(int)CBOEFields.Vega]);
            option.rho = float.Parse(fields[(int)CBOEFields.Rho]);
#endif
            return true;
        }

        void CheckOptionData()
        {
            foreach (Option option in options.Values)
            {
                var keys = option.optionDataList.Keys;
                DateTime firstDateTime = keys[0];
                DateTime lastDateTime = keys[keys.Count - 1];
                int aaa = 1;
            }
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
                DateOnly day = keyValuePair.Key;
                SortedList<TimeOnly, SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay = keyValuePair.Value;
                if (optionDataForDay.Count == 0)
                    Console.WriteLine($"No data for {day.ToString("d")}");
                else
                    Console.WriteLine($"Processing data for {day.ToString("d")}");

                foreach (var sortedListForTime in optionDataForDay)
                {
                    SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> optionDataForTime = sortedListForTime.Value;
                    Debug.Assert(optionDataForTime.Count > 0);
                    var item1 = optionDataForTime.Values[0].Item1; // StrikeIndex is a SortedList<int, optionData>
                    Debug.Assert(item1.Count > 0);
                    DateTime curDateTime = item1.Values[0].dt;
                    Console.WriteLine($"Testing at {curDateTime.ToString("HH.mm")}");

                    // loop through all exisiting positions, update their values, and see if they need to be adjusted or closed
#if PARFOR_ANALYZE
                    Parallel.ForEach(positions, ((int key, Position position)) => {
#else
                    foreach ((int key, Position position) in positions)
                    {
#endif
                        // compute value of position at current date/time
                        position.curValue = 0.0f;
                        position.curDelta = 0;

                        foreach (var kv in position.options)
                        {
                            var (option, quantity) = kv.Value;
                            OptionData curOption = option.optionDataList[curDateTime];
                            position.curDateTime = curDateTime;
                            position.curValue += quantity * curOption.mid * option.multiplier; // a negative value means a credit
                            position.curDelta += quantity * curOption.deltaIndex * 0.01f;
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
                                position.curDelta += order.quantity * curOption.delta;
                            }
                        }
#endif
                        // see if we need to close or adjust position
                        // first check the things that all optionData positions must check: maxLoss, profitTarget, minPositionDTE
                        bool closePosition = false;
                        float pl = position.curValue - position.entryValue;
                        int dit = day.DayNumber - DateOnly.FromDateTime(position.entryDateTime).DayNumber;
                        if (pl < maxLoss)
                        {
                            closePosition = true;
                        }
                        else if (position.curValue >= profitTarget)
                        {
                            closePosition = true;
                        }
                        else if (dit > maxDIT)
                        {
                            // close trade if dte of original trade is too small
                            closePosition = true;
                        }

                        if (closePosition)
                        {
                            bool removed = positions.TryRemove(key, out Position removedPosition);
                            Debug.Assert(removed);
                            bool added = closedPositions.TryAdd(key, removedPosition);
                            Debug.Assert(added);
                        }
                        else
                            position.adjust();
#if PARFOR_ANALYZE
                    });
#else
                    }
#endif
                    // now select new positions for this date and time
                    // first, just select expirations with 120 to 150 dte
                    ExpirationDate initialExpirationDate = day.AddDays(minDTEToOpen);
                    ExpirationDate finalExpirationDate = day.AddDays(maxDTEToOpen);

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
                            // each element of deltaList is a <Key, Value> pair, with the key being the delta and the value being the optionData
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
            // each element of deltaList is a <Key, Value> pair, with the key being the delta and the value being the optionData

            // find delta of -4  to -6
            var deltaList5 = strikeByDeltaList.Where(e => e.Key <= -delta5_minIndex && e.Key >= -delta5_maxIndex);
            Int32 deltaList5Count = deltaList5.Count();
            if (deltaList5.Count() == 0)
                return;

            // find delta of -14 to -16
            var deltaList15 = strikeByDeltaList.Where(e => e.Key <= -delta15_minIndex && e.Key >= -delta15_maxIndex);
            Int32 deltaList15Count = deltaList15.Count();
            if (deltaList15.Count() == 0)
                return;

            // find delta of -24 to -26
            var deltaList25 = strikeByDeltaList.Where(e => e.Key <= -delta25_minIndex && e.Key >= -delta25_maxIndex);
            Int32 deltaList25Count = deltaList25.Count();
            if (deltaList25.Count() == 0)
                return;

            foreach (var delta25kv in deltaList25)
            {
                OptionData opt25 = delta25kv.Value;
                foreach (var delta15kv in deltaList15)
                {
                    OptionData opt15 = delta15kv.Value;
                    foreach (var delta5kv in deltaList5)
                    {
                        OptionData opt5 = delta5kv.Value;

                        // calculate total delta
                        int iTranchMultiplier = STTBWBTranchMultiplier(opt25.underlying); // 1, 2, 3...
                        float totalDelta = iTranchMultiplier * (1 * opt25.delta - 2 * opt15.delta + 2 * opt5.delta);

                        // only create Trade if delta between -1.5 and 1.5
                        if (totalDelta <= 0.015f && totalDelta >= -0.015f)
                            AddSTTBWB(opt5, opt15, opt25, totalDelta, iTranchMultiplier);
                    }
                }
            }
        }

        // this determines whether we purchase 1-2-1, 2-4-2, 3-6-3, or 4-8-4; based on magnitude of underlying (larger means fewer options)
        int STTBWBTranchMultiplier(float underlying)
        {
            return 5 - (int)(underlying / 1500f);
        }

        void AddSTTBWB(OptionData opt5, OptionData opt15, OptionData opt25, float totalDelta, int iTranchMultiplier)
        {
            int ULQuantity = 1 * iTranchMultiplier;
            int SSQuantity = -2 * ULQuantity;
            int LLQuantity = 1 * ULQuantity;
            float fTranchMultiplier = (float)iTranchMultiplier;
            float value = fTranchMultiplier * (opt25.mid - 2f * opt15.mid + opt5.mid) * 100f; // a negative value means a credit
            float commission = fTranchMultiplier * 4f * BaseCommission;
            //float slippage = 4 * fTranchMultiplier * Slippage * 100.0f;
            //float costs = slippage + commission;

            Position position = new Position();
            position.positionType = PositionType.STTBWB;
            position.entryDelta = position.curDelta = totalDelta;
            position.entryValue = position.curValue = value;

            position.entryDateTime = opt25.dt;
            position.AddOption(opt5.option, LLQuantity);
            position.AddOption(opt15.option, SSQuantity);
            position.AddOption(opt25.option, ULQuantity);

            int newPositoonId = Interlocked.Increment(ref Position.positionId);
            bool added = positions.TryAdd(newPositoonId, position);
            Debug.Assert(added);

            Trade trade = new Trade();
            trade.tradeType = TradeType.BrokenWingButterfly;
            trade.dt = opt5.dt;
            trade.orders = new List<Order>();
            trade.commission = commission;
            position.trades.Add(trade);

            Order delta5Order = new Order();
            delta5Order.orderType = OrderType.Put;
            delta5Order.quantity = LLQuantity;
            delta5Order.option = opt5;
            trade.orders.Add(delta5Order);

            Order delta15Order = new Order();
            delta15Order.orderType = OrderType.Put;
            delta15Order.quantity = SSQuantity;
            delta15Order.option = opt15;
            trade.orders.Add(delta15Order);

            Order delta25Order = new Order();
            delta25Order.orderType = OrderType.Put;
            delta25Order.quantity = ULQuantity;
            delta25Order.option = opt25;
            trade.orders.Add(delta25Order);
        }

        // only for debugging IndexOfFirstDateLessThanOrEqualTo
        // actually finds first element greater than target and returns previous element
        public static int find_first_le(int[] a, int target)
        {
            int st = 0;
            int end = a.Length - 1;
            while (st <= end)
            {
                int mid = (st + end) / 2;
                if (a[mid] == target)
                    return mid;
                if (a[mid] < target)
                {
                    st = mid + 1;
                }
                else
                { // mid > target
                    end = mid - 1;
                }
            }
            return st - 1; // or return end + 1
        }

        // only for debugging IndexOfFirstDateGreaterThanOrEqualTo
        public static int find_first_ge(int[] a, int target)
        {
            int st = 0;
            int end = a.Length - 1;
            while (st <= end)
            {
                int mid = (st + end) / 2;
                if (a[mid] == target)
                    return mid;
                if (a[mid] < target)
                {
                    st = mid + 1;
                }
                else
                { // mid > target
                    end = mid - 1;
                }
            }
            Debug.Assert(st <= a.Length);
            if (st == a.Length)
                return -1;
            return st;
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
    using ExpirationDate = DateOnly;

    public static class TestSortedListExtensionClass
    {
        internal static void test()
        {
            var strikeIndex = new StrikeIndex();
            var deltaIndex = new DeltaIndex();
            var list = new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>();

            DateOnly d1 = new DateOnly(2021, 1, 1);
            list.Add(d1, (strikeIndex, deltaIndex));
            var i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);
            i1 = list.IndexOfFirstDateLessThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);

            DateOnly d2 = new DateOnly(2021, 1, 2);
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d2);
            Debug.Assert(i1 == -1);
            DateOnly d0 = new DateOnly(2020, 1, 1);
            i1 = list.IndexOfFirstDateLessThanOrEqualTo(d0);
            Debug.Assert(i1 == 0);

            list.Add(d2, (strikeIndex, deltaIndex));
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d2);
            Debug.Assert(i1 == 1);
            i1 = list.IndexOfFirstDateGreaterThanOrEqualTo(d1);
            Debug.Assert(i1 == 0);
        }
    }

    public static class SortedListExtensionClass
    {    
    internal static int IndexOfFirstDateGreaterThanOrEqualTo(this SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> options, ExpirationDate expirationDate)
        {
            Debug.Assert(options.Count > 0);
            int minIdx = 0;
            int maxIdx = options.Count - 1;

            // in each iteration of loop we increase minIdx or decrease maxIdx
            while (minIdx <= maxIdx)
            {
                int midIdx = (minIdx + maxIdx) / 2;
                DateOnly dt = options.ElementAt(midIdx).Key;
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
                    maxIdx = midIdx - 1;
                }
            }
            Debug.Assert(minIdx <= options.Count);
            if (minIdx == options.Count)
                return -1;
            return minIdx;
        }

        // returns index of first option in options whose expiration date is less than or equal to specified expirationDate
        internal static int IndexOfFirstDateLessThanOrEqualTo(this SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> options, ExpirationDate expirationDate)
        {
            Debug.Assert(options.Count > 0);
            int minIdx = 0;
            int maxIdx = options.Count - 1;

            // in each iteration of loop we increase minIdx or decrease maxIdx
            while (minIdx <= maxIdx)
            {
                int midIdx = (minIdx + maxIdx) / 2;
                DateOnly dt = options.ElementAt(midIdx).Key;
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

                    maxIdx = midIdx - 1;
                }
            }
            return minIdx - 1; // could be -1 if there is no element less than or equal to expirationDate
        }
    }
}


