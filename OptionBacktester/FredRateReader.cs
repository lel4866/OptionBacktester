/*
 * function to read FRED (St Louis Fed database) LIBOR interest rates, and a function to return the interpolated rate for
 * any duration, from 0 days to 360 days, for any given date in the FRED series back to the beginning of the FRED series,
 * which appears to be 2001-01-01 (some of the individual rate series go back further, this is the earliest for all the
 * series (rates_first_date)
 * 
 * The intention is that this will be used for things like the risk free rate for Black Scholes computations.
 * 
 * read_FRED_interest_rates() reads the FRED LIBOR rates from https://fred.stlouisfed.org/graph/fredgraph.csv
 * and creates a dictionary whose keys are the duration in days (1, 7, 30, 60, 90, 180, 360),
 * and whose values are another dictionary whose keys are a date and whose value is the rate in percent
 * 
 * the FRED LIBOR database goes back to 1986 or so, but has some missing days. Missing days are interpolated so there
 * will be a value for every day of the year from the start of the FRED series forward
 * 
 * After 12/31/2021, LIBOR rates will be replaced by SOFR and this code will be modified appropriately so the caller
 * doesn't need to know whether the rate comes from the LIBOR database or SOFR database
 * 
 * There is, in addition a function to read the monthly S&P 500 dividend yield from Nasdaq Data Link (formerly Quandl),
 * primarily for use in Black Scholes Merton option pricing formula that includes dividends.This is a monthly series,
 * which I interpolate to daily.Not sure if this interpolation is a good idea...
 * 
 * I make no guarantees of any kind for this program...use at your own risk
 * Lawrence E. Lewis
 */

using System;
using System.Collections.Generic;
using System.Collections.Concurrent; // to read FRED data in parallel
using System.Threading.Tasks;
using System.Diagnostics;
using Microsoft.Toolkit.HighPerformance; // for Span2D
using System.Linq;
using System.Net.Http;
using System.IO;

namespace ReadFredTreasuryRates;

public class FredRateReader {
    public const string version = "0.0.3";
    public const string version_date = "2021-09-11";

    static readonly Dictionary<int, string> seriesNames = new() {
        [1] = "USDONTD156N",
        [7] = "USD1WKD156N",
        [30] = "USD1MTD156N",
        [60] = "USD2MTD156N",
        [90] = "USD3MTD156N",
        [180] = "USD6MTD156N",
        [360] = "USD12MD156N"
    };

    static readonly private DateTime today = DateTime.Today;
    static readonly private string today_str = today.ToString("MM/dd/yyyy");
    public DateTime rates_last_date = today; // will hold earliest existing date over all the FRED series
    public DateTime rates_first_date = new(1980, 1, 1);  // will hold earliest existing date over all the FRED series
    public float[,] rates_array = new float[1, 1]; // the actual rate vector...1 value per day
    private ConcurrentDictionary<int, List<(System.DateTime, float)>> fred_interest_rates = new();
    public bool rates_valid = false;

    public FredRateReader(DateTime earliestDate) {
        Debug.Assert(earliestDate >= new DateTime(2000, 1, 1),
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is before 2000-01-01");
        Debug.Assert(earliestDate.Date <= today.Date,
            $"FredRateReader.cs: earliest date ({earliestDate.Date}) is after today ({today_str})");

        // read rate data from FRED into ConcurrentDictionary rates; rates is only used temporarily in this constructor
        // cleaned rate data for later use is saved in rates_array, which has 1 row for each day of data we are interested in,
        // and 1 column for each rate series that exists on FRED website (each duration)
        Parallel.ForEach(seriesNames, item => {
            GetFredDataFromUrl(item.Value, item.Key);
        });

#if false // for debugging
        WriteRawFredRates();
        System.Environment.Exit(-1); 
#endif

        // Get latest first date, earliest last date over all columns (durations) read from FRED. That is, set rates_first_date,
        // rates_last_date instance variables
        GetLatestFirstDate(earliestDate);

        // now create rates_array with 1 row for EVERY day (including weekends and holidays) between rates_first_date and
        // and rates_global_last_date (usually today), and 1 column for every duration between 0 and 360.
        CreateRatesArray();

        // copy data read form FRED into appropriate locations in rates_array. Since FRED data does not exist for all durations
        // or dates (like weekends or holidays), those values are created by piecewise linear interpolation
        FillRatesArray();

        fred_interest_rates.Clear(); // free up memory used to save data read from FRED

        // convert LIBOR rates read from FRED (based on 360 days/year) to those used in Black Scholes forumla (based on 365 days/year)
        ConvertRatesForBlackScholes();

        // set instance variable used to verify that rates_array is valid
        rates_valid = true;
    }

    private void GetFredDataFromUrl(string series_name, int duration) {
        List<(DateTime, float)> data = new(); // will contain rates for 1 duration

        // read data from FRED website
        Console.WriteLine("Reading " + series_name);
        string FRED_url = $"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_name}&cosd=1985-01-01&coed={today_str}";
        var hc = new HttpClient();
        Task<string> task = hc.GetStringAsync(FRED_url);
        string result_str = task.Result;
        hc.Dispose();

        // split string into lines, then into fields (should be just 2)
        string[] lines = result_str.Split('\n');
        bool header = true;
        foreach (string line in lines) {
            if (header) {
                header = false;
                continue;
            }
            if (line.Length == 0)
                continue;
            string[] fields = line.Split(',');
            if (fields.Length != 2)
                throw new InvalidFredDataException(series_name, line);
            if (!DateTime.TryParse(fields[0], out DateTime date))
                throw new InvalidFredDataException(series_name, line);
            float rate = float.NaN;
            if (fields[1] != ".")
                if (!float.TryParse(fields[1], out rate))
                    throw new InvalidFredDataException(series_name, line);
            data.Add((date, rate));
        }

        fred_interest_rates.TryAdd(duration, data); // fred_interest_rates contains rates for each duration read
    }

    // writes FRED interest rates read into fred_initerest_rates Dictionary to csv file for further analysis
    // there will be 1 row for every date including weekends and 1 column for each duration
    // missing data will be "missing"
    public void WriteRawFredRates(string filename) {
        //string filename = "../../../fred_data.csv";

        // since the actual data might have missing values (NaN's), and since each series might have different lengths,
        // we first copy data into a rectangular array
        Debug.Assert(seriesNames.Count == fred_interest_rates.Count); // since we'll be using seriesNames in header

        // get length of longest series and earliest first date over all series
        DateTime earliest_first_date = new DateTime(3000, 12, 31);
        foreach (var (duration, series) in fred_interest_rates) {
            if (series[0].Item1 < earliest_first_date)
                earliest_first_date = series[0].Item1;
        }
        int max_length = (today - earliest_first_date).Days;

        // allocate array that can hold all rates read
        int num_series = seriesNames.Count;
        var rates = new float[max_length, num_series];
        new Span2D<float>(rates).Fill(float.NaN); // crappy C# way to fill array with a value
        // fill in array
        int col = 0;
        foreach (var (duration, series) in fred_interest_rates) {
            foreach (var (date, rate) in series) {
                int row = (date - earliest_first_date).Days;
                Debug.Assert(row >= 0 && row < max_length);
                rates[row, col] = rate;
            }
            col++;
        }

        using StreamWriter file = new(filename);
        file.WriteLine("date,rate"); // write header
        for (int row = 0; row < max_length; row++) {
            // start new line with date based on row index
            DateTime date = earliest_first_date.AddDays(row);
            string line = $"{date.ToString("MM/dd/yyyy")}";
            for (col = 0; col < num_series; col++) {
                string valstr = ",missing";
                float rate = rates[row, col];
                if (!float.IsNaN(rate))
                    valstr = $",{rate}";
                line += valstr;
            }
            file.WriteLine(line); // start at 1 to skip leading comma
        }
    }

    // get latest first date over all series
    private void GetLatestFirstDate(DateTime earliestDate) {
        rates_first_date = earliestDate;
        foreach ((int duration, List<(DateTime, float)> series) in fred_interest_rates) {
            (DateTime first_date, float rate) = series[0];
            if (first_date > rates_first_date)
                rates_first_date = first_date;
        }

        Console.WriteLine();
        Console.WriteLine($"Starting date for risk free rate table will be: {rates_first_date.ToString("yyyy-MM-dd")}");
        Console.WriteLine($"Ending date for risk free rate table will be: {today_str}");
        Console.WriteLine();
    }

    // create rates_array with 1 row for EVERY day (including weekends and holidays) between global_first_date and
    // global_last_date (usually today), and 1 column for every duration between 0 and 360. Initialize with NaN's
    private void CreateRatesArray() {
        int num_rows = (rates_last_date - rates_first_date).Days + 1;
        rates_array = new float[num_rows, 361]; // initialized to 0f
        new Span2D<float>(rates_array).Fill(float.NaN); // crappy C# way to fill array with a value
    }

    // fill in rates_array from FRED data (in rates Dictionary) for durations 1, 7, 30, 60, 90, 180, 360.
    // Each of those columns will still have NaN's because fred_interest_rates dataframe does not have values
    // values for weekends or holidays. Also, most columns representing durations other than 1, 7, 30, etc.,
    // will still have all NaN's. Then, replace NaN's by interpolating using non-NaN's
    private void FillRatesArray() {
        int num_rows = rates_array.GetLength(0);

        // copy each column read from FRED database to rates_array. Remember, there are lots of columns in 
        // rates array for durations not available from FRED. These will have all NaN's
        foreach (var (duration, fred_data) in fred_interest_rates) {
            int index_of_first_rate = -1, index_of_last_rate = -1;
            foreach (var (date, rate) in fred_data) {
                if (date < rates_first_date)
                    continue;
                if (date > rates_last_date)
                    break;
                int date_index = (date - rates_first_date).Days;
                Debug.Assert(date_index >= 0);
                Debug.Assert(date_index < num_rows);
                if (!float.IsNaN(rate)) {
                    rates_array[date_index, duration] = rate;
                    // keep track of first and last rates which are not NaN
                    if (index_of_first_rate == -1)
                        index_of_first_rate = date_index;
                    index_of_last_rate = date_index; // will hold index of last value that is not a NaN
                }
            }

            // remove NaN's from rates_array in columns that got data from FRED database (weekends, holidays, etc)
            // that is, interpolate down the column
            InterpolateRatesArrayColumn(duration, index_of_first_rate, index_of_last_rate);
        }

        // remove NaN's from rates_array in columns that didn't get rates from FRED database
        // that is, interpolate across rows
        for (int row = 0; row < num_rows; row++)
            InterpolateRatesArrayRow(row);
    }

    // Replace NaN's in rates_array by piecewise linear interpolation using non-NaN values that surround the NaN's
    // This is only for columns that represent durations read from the FRED database (other columns will be all NaN's)
    private void InterpolateRatesArrayColumn(int duration, int index_of_first_rate, int index_of_last_rate) {
        int num_rows = rates_array.GetLength(0);
        // for this duration, get index of first NaN, index of last NaN
        if (index_of_first_rate == -1)
            throw new InvalidFredDataException(seriesNames[duration], $"All data is missing for duration {duration}");
        // fill front of column with first rate that is not NaN
        float first_rate = rates_array[index_of_first_rate, duration];
        for (int i = 0; i < index_of_first_rate; i++)
            rates_array[i, duration] = first_rate;
        // fill back of column with last rate that is not NaN
        float last_rate = rates_array[index_of_last_rate, duration];
        for (int i = index_of_last_rate + 1; i < num_rows; i++)
            rates_array[i, duration] = last_rate;

        // interpolate between interior NaN's for this duration
        int row = index_of_first_rate;
        Debug.Assert(!float.IsNaN(rates_array[0, duration]));
        while (row < index_of_last_rate) {
            // find index of first NaN
            if (!float.IsNaN(rates_array[row, duration])) {
                row++;
                continue;
            }
            // we found a NaN in rates_array[row, duration]

            // find next non-NaN. This will be found because we filled back of array with non-NaN's
            int j;
            for (j = row + 1; j < index_of_last_rate; j++) {
                if (!float.IsNaN(rates_array[j, duration]))
                    break;
            }
            Debug.Assert(j < index_of_last_rate); // assert we found an Nan
            float next_value = rates_array[j, duration]; // next_value is first rate that is not NaN after Nan in row
            Debug.Assert(!float.IsNaN(next_value));

            // interpolate between non-NaN in row-1 and non-Nan in j;
            int index_range = j - (row - 1);
            float value = rates_array[row - 1, duration]; // starting value
            float value_range = next_value - value;
            float value_increment = value_range / index_range;

            for (int nan_index = row; nan_index < j; nan_index++) {
                value = value + value_increment;
                rates_array[nan_index, duration] = value;
            }

            row = j + 1; // set row to next row to look for NaN (since we know row j is not NaN)
        }
    }

    // Each row in rates_array contains values for all durations for a given date, but durations that were not read from FRED
    // will have NaN's. Replace those NaN's by piecewise linear interpolation using non-NaN values that surround the NaN's
    // We know that the beginning and ending elements of each row ARE NOT NaN's because they represent durations of 1 and 360
    // that were read from FRED. Ccolumn 0 of rates_array is unused and remains all NaN
    private void InterpolateRatesArrayRow(int row) {
        int next_non_nan, duration = 1;
        while (duration <= 360) {
            // find index of first NaN
            if (!float.IsNaN(rates_array[row, duration])) {
                duration++;
                continue;
            }
            // we found a NaN in rates_array[row, duration]

            // find next non-NaN. This will be found because last column (360) has no NaN's
            for (next_non_nan = duration + 1; next_non_nan <= 360; next_non_nan++) {
                if (!float.IsNaN(rates_array[row, next_non_nan]))
                    break;
            }
            Debug.Assert(next_non_nan <= 360); // assert we found an Nan
            float next_value = rates_array[row, next_non_nan]; // next_value is first rate that is not NaN after Nan in duration
            Debug.Assert(!float.IsNaN(next_value));

            // interpolate between non-NaN in column duration-1 and non-Nan in duration;
            int index_range = next_non_nan - (duration - 1);
            float value = rates_array[row, duration - 1]; // starting value
            float value_range = next_value - value;
            float value_increment = value_range / index_range;

            for (int nan_index = duration; nan_index < next_non_nan; nan_index++) {
                value = value + value_increment;
                rates_array[row, nan_index] = value;
            }

            duration = next_non_nan + 1; // set duration to next column to look for NaN (since we know column j is not NaN)
        }
    }

    private void ConvertRatesForBlackScholes() {
        int num_rows = rates_array.GetLength(0);
        for (int row = 0; row < num_rows; row++)
            for (int duration = 1; duration <= 360; duration++)
                rates_array[row, duration] = (float)(360.0 / duration * Math.Log(1.0 + rates_array[row, duration] * duration / 365.0));
    }

    // make sure rates_array has reasonable values
    public bool RateSanityCheck() {
        const float min_rate = 0;
        const float max_rate = 30f;
        const float max_rate_change = 5.00f;
        // make sure risk free rates greater than 0 and less than 30
        // make sure the change between any days is less than 0.5
        bool passed = true;

#if false // for debugging
        // display all rates for duraion 1
        for (int i = 0; i < rates_array.GetLength(0); i++) 
            Console.WriteLine($"rate[{i},1] = {rates_array[i, 1]}");
#endif

        // initialize prior_rates for each duration
        float[] prior_rates = new float[rates_array.GetLength(1)];
        for (int i = 1; i < rates_array.GetLength(1); i++)
            prior_rates[i] = rates_array[0, i];

        for (int row = 0; row < rates_array.GetLength(0); row++) {
            for (int duration = 1; duration < rates_array.GetLength(1); duration++) {
                DateTime date = rates_first_date.AddDays(row);
                float rate = rates_array[row, duration];
                if (rate <= min_rate || rate >= max_rate) {
                    Console.WriteLine($"ReadFredTreasuryRates.py:rate_sanity_check: rate for duration: {duration}, date: {date.Date}, is not reasonable: {rate}");
                    passed = false;
                    continue;
                }

                float change_in_rate = Math.Abs(rate - prior_rates[duration]);
                if (change_in_rate > max_rate_change) {
                    Console.WriteLine($"ReadFredTreasuryRates.py:rate_sanity_check: change in rate for duration: {duration}, date: {date.Date}, is not reasonable: {change_in_rate}");
                    passed = false;
                }
                prior_rates[duration] = rate;
            }
        }

        return passed;
    }

    // main function used to get a risk free rate for a specified date and duration
    public float RiskFreeRate(DateTime requestedDate, int duration) {
        Debug.Assert(requestedDate.Date >= rates_first_date.Date,
            $"FredRateReader.cs::RiskFreeRate: requested date ({requestedDate.Date}) is before earliest available ({rates_first_date.Date}).");
        Debug.Assert(requestedDate.Date <= rates_last_date.Date,
            $"FredRateReader.cs::RiskFreeRate: requested date ({requestedDate.Date}) is after latest available ({rates_last_date.Date}).");
        Debug.Assert(duration > 0 && duration <= 360,
            $"FredRateReader.cs::RiskFreeRate: duration must be between 1 and 360, not {duration}.");

        int date_index = (requestedDate - rates_first_date).Days;
        return rates_array[date_index, duration];
    }
}

public class SP500DividendYieldReader {
    static readonly private DateTime today = DateTime.Today;
    public bool dividends_valid = false;
    public float[] dividends_array = new float[1];  // vector containing sp500 dividend yield in percent
    public DateTime dividends_first_date;  // will hold earliest existing date in dividend_array
    public DateTime dividends_last_date = today; // will hold latest existing date in dividend_array
    private const string url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_DIV_YIELD_MONTH.csv?api_key=r1LNaRv-SYEyP9iY8BKj";

    public SP500DividendYieldReader(DateTime earliestDate) {
        var hc = new HttpClient();
        Task<string> task = hc.GetStringAsync(url);
        string result_str = task.Result;
        hc.Dispose();

        // split string into lines, then into fields (should be just 2)
        // for this data from Nasdaq Data Link (formerly Quandl) 1st line is newest
        string[] lines = result_str.Split('\n');

        // parse lines into date and dividend and add to a sorted dictionary
        bool header = true;
        SortedDictionary<DateTime, float> dividends = new();
        foreach (string line in lines) {
            // skip header
            if (header) {
                header = false;
                continue;
            }
            // skip blank lines
            if (line.Length == 0)
                continue;

            // parse line: date, dividend
            string[] fields = line.Split(',');
            if (fields.Length != 2)
                throw new InvalidDividenDataException(line);
            if (!DateTime.TryParse(fields[0], out DateTime date))
                throw new InvalidDividenDataException(line);
            if (!float.TryParse(fields[1], out float dividend))
                throw new InvalidDividenDataException(line);

            if (date >= earliestDate)
                dividends[date] = dividend;
        }

        // if no dividends found, throw exception
        if (dividends.Count == 0)
            throw new InvalidDividenDataException($"No dividend data available from Nasdaq Data Link later than {earliestDate.Date}");

        // now allocate dividends array (1 row for every day, including weekends and holidays)
        CreateDividendsArray(dividends);

        // fill dividends_array with values read from Nasdaq Data Link and interpolate away any NaN's
        FillDividendsArray(dividends);

        // set instance variable used to verify that rates_array is valid
        dividends_valid = true;
    }

    // create rates_array with 1 row for EVERY day (including weekends and holidays) between global_first_date and
    // global_last_date (usually today), and 1 column for every duration between 0 and 360. Initialize with NaN's
    void CreateDividendsArray(SortedDictionary<DateTime, float> dividends) {
        Debug.Assert(dividends.Count > 0);
        (dividends_first_date, _) = dividends.First();
        int num_rows = (dividends_last_date - dividends_first_date).Days + 1;
        dividends_array = new float[num_rows]; // initialized to 0f
        new Span<float>(dividends_array).Fill(float.NaN); // crappy C# way to fill array with a value
    }

    // Each row in rates_array contains values for all durations for a given date, but durations that were not read from FRED
    // will have NaN's. Replace those NaN's by piecewise linear interpolation using non-NaN values that surround the NaN's
    // We know that the beginning and ending elements of each row ARE NOT NaN's because they represent durations of 1 and 360
    // that were read from FRED. Ccolumn 0 of rates_array is unused and remains all NaN
    void FillDividendsArray(SortedDictionary<DateTime, float> dividends) {
        // fill dividends_array with values read from Nasdaq Data Link (they are in dividends SortedDictionary)
        int index_of_first_dividend = int.MaxValue, index_of_last_dividend = -1; // will hold indices of first, last non-NaN
        foreach (var (date, dividend) in dividends) {
            int date_index = (date - dividends_first_date).Days;
            dividends_array[date_index] = dividend;

            if (date_index < index_of_first_dividend)
                index_of_first_dividend = date_index;
            if (date_index > index_of_last_dividend)
                index_of_last_dividend = date_index;
        }
        Debug.Assert(index_of_first_dividend >= 0);
        Debug.Assert(index_of_last_dividend >= 0);

        // fill front of array with first dividend that is not NaN
        int num_rows = dividends_array.Length;
        float first_dividend = dividends_array[index_of_first_dividend];
        for (int i = 0; i < index_of_first_dividend; i++)
            dividends_array[i] = first_dividend;
        // fill back of column with last rate that is not NaN
        float last_dividend = dividends_array[index_of_last_dividend];
        for (int i = index_of_last_dividend + 1; i < num_rows; i++)
            dividends_array[i] = last_dividend;

        // interpolate between interior NaN's for this duration
        Debug.Assert(!float.IsNaN(dividends_array[0]));
        int next_non_nan, row = index_of_first_dividend; // row will hold index of first non-NaN preceeding NaN
        while (row < index_of_last_dividend) {
            // find index of first NaN
            if (!float.IsNaN(dividends_array[row])) {
                row++;
                continue;
            }
            // we found a NaN in dividends_array[row]

            // find next non-NaN
            for (next_non_nan = row + 1; next_non_nan < num_rows; next_non_nan++) {
                if (!float.IsNaN(dividends_array[next_non_nan]))
                    break;
            }
            Debug.Assert(next_non_nan < num_rows); // assert we found an Nan
            float next_value = dividends_array[next_non_nan]; // next_value is first rate that is not NaN after Nan in duration
            Debug.Assert(!float.IsNaN(next_value));

            // interpolate between non-NaN value in row row-1 and non-Nan value in row next_non_nan;
            int index_range = next_non_nan - (row - 1);
            float value = dividends_array[row - 1]; // starting value
            float value_range = next_value - value;
            float value_increment = value_range / index_range;

            for (int nan_index = row; nan_index < next_non_nan; nan_index++) {
                value = value + value_increment;
                dividends_array[nan_index] = value;
            }

            row = next_non_nan + 1; // set duration to next column to look for NaN (since we know column j is not NaN)
        }
    }

    public bool DividendSanityCheck() {
        // make sure dividend yield is greater than 0% and less than 5%
        // make sure the change between any days is less than 0.2%
        bool passed = true;
        float prior_dividend = dividends_array[0];
        foreach (var dividend in dividends_array) {
            if (dividend <= 0 || dividend >= 5) {
                DateTime date = dividends_first_date.AddDays(1);
                Console.WriteLine($"ReadFredTreasuryRates.py:dividend_sanity_check: rate for {date.Date}, is not reasonable: {dividend}");
                passed = false;
                continue;
            }

            float dividend_change = Math.Abs(dividend - prior_dividend);
            prior_dividend = dividend;
            if (dividend_change >= 1) {
                DateTime date = dividends_first_date.AddDays(1);
                Console.WriteLine($"ReadFredTreasuryRates.py:dividend_sanity_check: change in dividend for {date.Date}, is not reasonable: {dividend_change}");
                passed = false;
            }
        }

        return passed;
    }

    // main function used to get a dividend yield in percent for a specified date
    public float DividendYield(DateTime requestedDate) {
        Debug.Assert(requestedDate >= dividends_first_date,
            $"FredRateReader.cs::DividendYield: requested date ({requestedDate.Date}) is before earliest available ({dividends_first_date.Date}).");
        Debug.Assert(requestedDate <= dividends_last_date,
            $"FredRateReader.cs::DividendYield: requested date ({requestedDate.Date}) is after latest available ({dividends_last_date.Date}).");

        int date_index = (requestedDate - dividends_first_date).Days;
        return dividends_array[date_index];
    }
}

[Serializable]
public class InvalidFredDataException : Exception {
    internal InvalidFredDataException() { }
    internal InvalidFredDataException(string series_name, string row)
        : base($"Invalid data in rate series: {series_name}: {row}") { }
}

[Serializable]
public class InvalidDividenDataException : Exception {
    internal InvalidDividenDataException() { }
    internal InvalidDividenDataException(string row)
        : base($"Invalid data in S&P 500 Dividend Yield series: {row}") { }
}
