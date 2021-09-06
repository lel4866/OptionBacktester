﻿#define NO_CALLS
#define PARFOR_READDATA
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

namespace OptionBacktester
{
    using StrikeIndex = SortedList<int, OptionData>;
    using DeltaIndex = SortedList<int, OptionData>;
    using ExpirationDate = DateTime;

    class Option
    {
        internal string root;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal float multiplier = 100f; // converts quantity to dollars
        internal SortedList<DateTime, OptionData> optionData = new SortedList<DateTime, OptionData>();
    }

    class OptionData
    {
        internal Option option;
        internal DateTime dt;
        internal string root;
        internal DateTime expiration;
        internal int strike;
        internal LetsBeRational.OptionType optionType;
        internal float bid;
        internal float ask;
        internal float mid;
        internal float underlying;
        internal int dte;
        internal float riskFreeRate;
        internal float dividend;
        internal float iv;
        internal int delta100 = -10000; // delta in percent times 100; int so it makes a good index
        internal float multiplier = 100f; // converts quantity to dollars
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

    class Position
    {
        // currently held options with key of (root, expiration, strike, type); each (Option, int) is a reference to an Option, and the quantity in the position
        internal SortedList<(string, DateTime, int, LetsBeRational.OptionType), (Option, int)> options = new SortedList<(string, DateTime, int, LetsBeRational.OptionType), (Option, int)>();

        internal PositionType positionType; // the original PositionType of the position
        //internal List<(Option, int)> options; // a list of Options and quantities that make up the "current" position; 
        internal List<Trade> trades = new List<Trade>(); // trades[0] contains the initial trade...so the Orders in that trade are the initial position
        internal DateTime entryDate; // so we can easily calculate dte
        internal float entryValue;
        internal float curValue;
        internal float entryDelta;
        internal float curDelta;
        internal bool closePosition;

        // add an option to options collection if it is not already there.
        // if it is, adjust quantity
        // returns false if option quantity now 0 (and removes the option from the options collection)
        internal bool AddOption(Option option, int quantity)
        {
            Debug.Assert(quantity != 0);

            var key = (option.root, option.expiration, option.strike, option.optionType);
            (Option, int) value ;
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

    enum TradeType
    {
        None,
        BalancedButterfly,
        BrokenWingButterfly,
        PCS,
        PDS,
        Single
    }

    class Trade
    {
        internal TradeType tradeType = TradeType.None;
        internal DateTime dt; // when orders placed and filled (this is a backtest...we assume orsers filled instantly)
        internal float commission; // total commission for all orders
        internal List<Order> orders = new List<Order>(); // each order is for a quantity of a single option
    }

    enum OrderType
    {
        None,
        Put,
        Call,
        Stock
    }

    class Order
    {
        internal OrderType orderType = OrderType.None;
        internal OptionData option; // reference to option at entry
        internal int quantity;
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
        const int deepInTheMoneyAmount = 100; // # of SPX points at which we consider option "depp in the money"
        const int minStrike = 500;
        const int maxStrike = 3500;
        const int minDTE = 120;
        const int maxDTE = 150;
        const int minPositionDTE = 30;
        const float maxLoss = -2000f;
        const float profitTarget= 1000f;

        const float Slippage = 0.05f; // from mid
        const float BaseCommission = 0.65f + 0.66f;
        const string DataDir = @"C:\Users\lel48\OneDrive\Documents\CboeDataShop/SPX/"; // CBOE DataShop data
        CultureInfo provider = CultureInfo.InvariantCulture;

        Dictionary<DateTime, float> RiskFreeRate = new Dictionary<DateTime, float>();
        Dictionary<DateTime, float> SP500DivYield = new Dictionary<DateTime, float>();
        List<Position> positions = new List<Position>();
        int optionCount = 0; // # of valid Options;
        System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();

        // For selecting new positions based on dte and delta: [Date][Time][Dte][Delta]; only 1 option at each [date][time][dte][delta]; only 1 root for now
        //SortedList<DateTime, List<SortedList<int, SortedList<int, Option>>>> OptionData = new SortedList<DateTime, List<SortedList<int, SortedList<int, Option>>>>();

        // for updating current positions based on ExpirationDate and Strike (strike_Expiration_Index), and
        // for selecting new positions based on dte and delta (dte_delta_Index)
        //SortedList<DateTime, List<OptionIndexes>> OptionData = new SortedList<DateTime, List<OptionIndexes>>(); // DateTime is a Date; List<Index> is in order of time of day in fixed 15 minute increments

        // First List is in order of Date; Second List is in order of time of day in fixed 15 minute increments
        // Second List naturally sorted by Time because data in each file must be in order of time (it is checked to make sure)
        // StrikeIndex = SortedList<int, Option> is for updating existing positions given expiration date and strike
        // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
        SortedList<DateTime, List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> OptionData = new SortedList<DateTime, List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>>();

        static void Main(string[] args)
        {
#if false
            double price = 25.01;
            double r = 0.0012; // risk free rate(1 year treasury yield)
            double d = 0.0194; // trailing 12 - month sp500 dividend yield
            double t = 120.0 / 365.0; // days to expiration / days in year
            double s = 1843.37; // underlying SPX price
            double K = 1725.0; // strike price
            double iv = LetsBeRational.ImpliedVolatility(price, s, K, t, r, d, LetsBeRational.OptionType.Put);
            double delta = LetsBeRational.Delta(s, K, t, r, iv, d, LetsBeRational.OptionType.Put);
#endif
            var pgm = new Program();
            pgm.run();
            int a = 1;
        }

        void run()
        {
            watch.Start();

            ReadRiskFreeRates(@"C:/Users/lel48/TreasuryRates/");
            ReadSP500DivYield(@"C:/Users/lel48/TreasuryRates/MULTPL-SP500_DIV_YIELD_MONTH.csv");
            ReadDataAndComputeGreeks();

            watch.Stop();
            Console.WriteLine($"Time to read data and compute iv,delta: {0.001 * watch.ElapsedMilliseconds / 60.0} minutes");

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
#if false
            List<string> myList = new List<string>();
            IEnumerable<string> results = myList.Where(s => s == "abc");
            SortedList<int, Option> mySortList = new SortedList<int, Option>();
            IEnumerable<KeyValuePair<int, Option>> res = mySortList.Where(i => i.Key > 30 && i.Key < 60);
#endif
            // CBOEDataShop 15 minute data (900sec); data can be stored hierarchically (by year, etc)
            string[] zipFileNameArray = Directory.GetFiles(DataDir, "UnderlyingOptionsIntervalsQuotes_900sec_??????????.zip", SearchOption.AllDirectories);
            Array.Sort(zipFileNameArray);
#if false
            // first List is in order of Date; Second List is in order of time of day in fixed 15 minute increments
            // StrikeIndex = SortedList<int, Option> is for updateing existing positions given expiration date and strike
            // DeltaIndex = SortedList<int, Option> is for scanning for new positions given initial dte and initial delta
            List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>> OptionData = new List<List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>>();
#endif
            // initialize outer List ordered by Date with sub List (ordered by time)
            foreach (string zipFileName in zipFileNameArray)
            {
                DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));

                List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> timeSortedList = new List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>>();
                OptionData.Add(zipDate, timeSortedList);
            }
#if PARFOR_READDATA
            Parallel.ForEach(zipFileNameArray, (zipFileName) =>
            {
#else
            foreach (string zipFileName in zipFileNameArray)
            {
#endif
                string line;
                bool validOption;
                OptionData option = null;
                StrikeIndex optionDataForStrike;
                DeltaIndex optionDataForDelta;

                using (ZipArchive archive = ZipFile.OpenRead(zipFileName))
                {
                    if (archive.Entries.Count != 1)
                        throw new Exception($"There must be only one entry in each zip file: {zipFileName}");
                    string fileName = archive.Entries[0].Name;
                    Console.WriteLine($"Processing file: {fileName}");
                    ZipArchiveEntry zip = archive.Entries[0];
                    DateTime zipDate = DateTime.Parse(zipFileName.Substring(zipFileName.Length - 14, 10));
#if false
                    var testdt = new DateTime(2014, 2, 21);
                    if (zipDate.Date == testdt)
                    {
                        int zzz = 1;
                    }
#endif
                    List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay = OptionData[zipDate]; // optionDataForDay is 3d List[time][dte][delta]
                    using (StreamReader reader = new StreamReader(zip.Open()))
                    {
                        line = reader.ReadLine(); // skip header
                        validOption = true;
                        int rowIndex = 0;
                        bool newDateTime = false;
                        DateTime curDateTime = new DateTime();
                        while ((line = reader.ReadLine()) != null)
                        {
                            ++rowIndex;
                            if (validOption)
                                option = new OptionData();
                            validOption = ParseOption(line, option, zipDate, ref curDateTime, ref newDateTime);
                            if (validOption)
                            {
#if false
                                if (option.strike == 1725)
                                {
                                    int aaa = 1;
                                    if (option.dte == 120)
                                    {
                                        int aba = 1;
                                    }
                                }
                                if (option.strike == 550 && option.dte == 16)
                                {
                                    int qr = 1;
                                }
#endif
                                ComputeGreeks(option);
                                optionCount++;
                                // add option to various collections
                                if (newDateTime)
                                {
                                    // if first option of day, need to create collections
                                    var optionDataForExpirations = new SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>();
                                    optionDataForDay.Add(optionDataForExpirations);

                                    // make sure we have dummy entries in optionDataForExpirations SortedList for initial expirations for when we're searching for new positions
                                    // for instance, if we're searching for STT's between 120 and 150 dte, we need entries in optionDataForExpirations for today+120 days
                                    // hard code this for now...eventually, add a List of dtes to add

                                    // add dummy entry for 120 dte - the first dte for STT's we are opening
                                    // the dummy entry has no StrikeIndex or DeltaIndex, which is how we know it's a dummy
                                    // if we actually get an option at 120 dte, then it will replace dummy
                                    DateTime exp120 = option.expiration.AddDays(120);
                                    optionDataForStrike = optionDataForDelta = null;
                                    optionDataForExpirations.Add(exp120, (optionDataForStrike, optionDataForDelta));

                                    // we also need dummy entries in optionDataForExpirations for the start of the delta search. For instance, if we want to find options
                                    // with a delta of 5, we start the search at options with deltas of 3.5, so we must have a dummy (or real) option with a delta of 3.5
                                    // so this initial IndexOfKey will not throw an exception
                                }

                                // now add current option to collections
                                (StrikeIndex, DeltaIndex) optionDataForExpiration;
                                var optionDataForTime = optionDataForDay.Last();
                                bool expirationFound = optionDataForTime.TryGetValue(option.expiration, out optionDataForExpiration);
                                if (!expirationFound)
                                {
                                    optionDataForStrike = new StrikeIndex();
                                    optionDataForDelta = new DeltaIndex();
                                    optionDataForTime.Add(option.expiration, (optionDataForStrike, optionDataForDelta));
                                    optionDataForStrike.Add(option.strike, option);
                                    optionDataForDelta.Add(option.delta100, option);
                                }
                                else
                                {
                                    optionDataForStrike = optionDataForExpiration.Item1;
                                    if (optionDataForStrike == null)
                                    {
                                        // this means we are replacing a dummy option
                                        Debug.Assert(optionDataForExpiration.Item2 == null);
                                        optionDataForStrike = new StrikeIndex();
                                        optionDataForDelta = new DeltaIndex();
                                        //optionDataForTime.Add(option.expiration, (optionDataForStrike, optionDataForDelta));
                                        optionDataForStrike.Add(option.strike, option);
                                        optionDataForDelta.Add(option.delta100, option);
                                        continue;
                                    }
                                    if (optionDataForStrike.ContainsKey(option.strike))
                                    {
                                        Console.WriteLine("Duplicate Strike: {option.strike}");
                                        continue;
                                    }
                                    optionDataForStrike.Add(option.strike, option);

                                    optionDataForDelta = optionDataForExpiration.Item2;

                                    // this is for reaaly out of the money puts, where delta values are 0.0001 or less
                                    // the roundoff algo will make them all -1...that is, duplicates
                                    //if (option.delta100 > -500) {
                                    if (option.delta100 != 0 && Math.Abs(option.delta100) != 10000)
                                    {
                                        if (option.optionType == LetsBeRational.OptionType.Call)
                                        {
                                            Debug.Assert(option.delta100 > 0);
                                            while (optionDataForDelta.ContainsKey(option.delta100))
                                                option.delta100++;

                                        }
                                        else
                                        {
                                            Debug.Assert(option.optionType == LetsBeRational.OptionType.Put);
                                            Debug.Assert(option.delta100 < 0);
                                            while (optionDataForDelta.ContainsKey(option.delta100))
                                                option.delta100--;
                                        }
                                    }
                                    //}
                                    if (optionDataForDelta.ContainsKey(option.delta100))
                                    {
                                        if (option.delta100 != 0 && Math.Abs(option.delta100) != 10000)
                                        {
                                            Console.WriteLine("Duplicate Delta: {option.delta100}");
                                        }
                                        continue;
                                    }

                                    // for really OTM strikes, with deltas <= -1, the round off algorithm will make them all -1
                                    /// so...we adjust here to avoid that
                                    if (option.delta100 == -1)
                                    {

                                    }
                                    optionDataForDelta.Add(option.delta100, option);
                                }

                            }
                        }
                    }
                }
#if PARFOR_READDATA
            });
#else
            }
#endif
            // now 
            int aa = 1;
        }

        static bool ParseOption(string line, OptionData option, DateTime zipDate, ref DateTime curDateTime, ref bool newDateTime)
        {
            Debug.Assert(option != null);
            newDateTime = false;

            string[] fields = line.Split(',');

            option.root = fields[2];

            // for now, don't use weekly or quarterly options
            if (option.root != "SPX")
                return false;

            option.optionType = fields[5].Trim() == "p" ? LetsBeRational.OptionType.Put : LetsBeRational.OptionType.Call;
#if NO_CALLS
            // we're not interested in Calls right now
            if (option.optionType == LetsBeRational.OptionType.Call)
                return false;
#endif
            option.strike = (int)(float.Parse(fields[4]) + 0.001f); // +.001 to prevent conversion error
                                                                    // for now, only conside strikes with even multiples of 25
            if (option.strike % 25 != 0)
                return false;
            if (option.strike < minStrike || option.strike > maxStrike)
                return false;

            option.underlying = float.Parse(fields[15]);

            // we're not interested in ITM strikes right now
            if (option.strike >= option.underlying)
                return false;

            //row.dt = DateTime.ParseExact(fields[1], "yyyy-MM-dd HH:mm:ss", provider);
            option.dt = DateTime.Parse(fields[1]);
            // you can have many, many options at same date/time, but once date/time increments, you can't go backwards
            Debug.Assert(option.dt.Date == zipDate);
            Debug.Assert(option.dt >= curDateTime);

            //row.expiration = DateTime.ParseExact(fields[3], "yyyy-mm-dd", provider);
            option.expiration = DateTime.Parse(fields[3]);

            TimeSpan tsDte = option.expiration.Date - option.dt.Date;
            option.dte = tsDte.Days;
            if (option.dte == 0)
                return false;

            // we're not interested in dte greater than 180 days
            if (option.dte > maxDTE)
                return false;

            option.ask = float.Parse(fields[14]);
            option.bid = float.Parse(fields[12]);
            if (option.ask == 0f && option.bid == 0f)
                return false;
            option.mid = (0.5f * (option.bid + option.ask));

            // this test must happen AFTER any "return false;" statements
            if (option.dt > curDateTime)
            {
                curDateTime = option.dt;
                newDateTime = true;
            }
            return true;
        }

        void ComputeGreeks(OptionData option)
        {
            // compute iv and delta of option
            double t = option.dte / 365.0;
            double r = RiskFreeRate[option.dt.Date];
            double d = SP500DivYield[option.dt.Date];
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
                option.delta100 = (int)(10000.0 * delta);
                if (Math.Abs(option.delta100) > 10000)
                {
                    int cc = 1;
                }
                Debug.Assert(Math.Abs(option.delta100) <= 10000);
#if false
                if (option.delta100 == -1) {
                    int bb = 1;
                }
#endif
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
            if (optionCount == 0)
            {
                Console.WriteLine("No valid data read. Backtest ended.");
                return;
            }
            Console.WriteLine($"Starting backtest from {OptionData.Keys[0].ToString("d")} to {OptionData.Keys[OptionData.Count - 1].ToString("d")}");

            // start at first date/time of data
            foreach (var keyValuePair in OptionData)
            {
                DateTime day = keyValuePair.Key;
                List<SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)>> optionDataForDay = keyValuePair.Value;
                if (optionDataForDay.Count == 0)
                    Console.WriteLine($"No data for {day.ToString("d")}");
                else
                    Console.WriteLine($"Processing data for {day.ToString("d")}");

                foreach (SortedList<ExpirationDate, (StrikeIndex, DeltaIndex)> optionDataForTime in optionDataForDay)
                {
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
                            position.curValue += quantity * curOption.mid * 100.0f; // a negative value means a credit
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
                    DateTime initialExpirationDate = day.AddDays(minDTE);
                    DateTime finalExpirationDate = day.AddDays(maxDTE);
                    int startIndex = optionDataForTime.IndexOfKey(initialExpirationDate);
                    int endIndex = optionDataForTime.IndexOfKey(finalExpirationDate);
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

        // populate: Dictionary<DateTime, float> RiskFreeRate;
        // reads from https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield

        void ReadRiskFreeRates(string rfdir)
        {
            DateTime prevDate = new DateTime();
            var yearArray = new string[] { "2014", "2015", "2016", "2017", "2018", "2019", "2020" };
            bool findFirstDate = true;
            float prevRate = 0.0f;
            foreach (var year in yearArray)
            {
                var filename = $"{rfdir}{year}.csv";
                string[] lines = System.IO.File.ReadAllLines(filename);
                int lineno = 0;
                foreach (string line in lines)
                {
                    lineno++;
                    string[] fields = line.Split(',');
                    if (fields.Length != 2)
                        throw new Exception($"Each line in risk free interest rate file must contain 2 values (date, rate): {lineno}");
                    var date = DateTime.Parse(fields[0]);
                    var rate = float.Parse(fields[1]);

                    // save date and rate of first line
                    if (findFirstDate)
                    {
                        findFirstDate = false;
                        prevDate = date.AddDays(-1.0);
                        prevRate = rate;
                    }

                    // add entries for which there are no lines in file
                    DateTime yesterday = date.AddDays(-1.0);
                    while (prevDate < yesterday)
                    {
                        prevDate = prevDate.AddDays(1.0);
                        RiskFreeRate.Add(prevDate, prevRate * 0.01f);
                    }

                    Debug.Assert(date == date.Date);
                    RiskFreeRate.Add(date, rate * 0.01f);
                    prevDate = date;
                    prevRate = rate;
                }
            }
        }

        // populate: Dictionary<DateTime, float> SP500DivYield;
        // reads file downloaded from Quandl: https://www.quandl.com/data/MULTPL/SP500_DIV_YIELD_MONTH-S-P-500-Dividend-Yield-by-Month
        void ReadSP500DivYield(string filename)
        {
            DateTime prevDate = new DateTime();
            bool findFirstDate = true;
            float prevYield = 0.0f;

            string[] lines = System.IO.File.ReadAllLines(filename);
            int lineno = 0;
            foreach (string line in lines)
            {
                lineno++;
                string[] fields = line.Split(',');
                if (fields.Length != 2)
                    throw new Exception($"Each line in sp500 dividend yield file must contain 2 values (date, yield): {lineno}");
                var date = DateTime.Parse(fields[0]);
                var yield = float.Parse(fields[1]);

                // save date and rate of first line
                if (findFirstDate)
                {
                    findFirstDate = false;
                    prevDate = date.AddDays(-1.0);
                    prevYield = yield;
                }

                // add entries for which there are no lines in file
                DateTime yesterday = date.AddDays(-1.0);
                while (prevDate < yesterday)
                {
                    prevDate = prevDate.AddDays(1.0);
                    SP500DivYield.Add(prevDate, prevYield * 0.01f);
                }

                Debug.Assert(date == date.Date);
                SP500DivYield.Add(date, yield * 0.01f);
                prevDate = date;
                prevYield = yield;
            }
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
