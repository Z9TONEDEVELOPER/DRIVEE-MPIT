using System;
using Avalonia.Data.Converters;

namespace DriveeDataSpace.Desktop.Converters;

public class BoolToProgressValueConverter : IValueConverter
{
    public static readonly BoolToProgressValueConverter Instance = new();

    public object Convert(object? value, Type targetType, object? parameter, System.Globalization.CultureInfo culture)
    {
        if (value is bool booleanValue)
        {
            return booleanValue ? 100.0 : 0.0;
        }
        
        return 0.0;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, System.Globalization.CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}