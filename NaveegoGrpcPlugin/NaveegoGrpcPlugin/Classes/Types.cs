using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NaveegoGrpcPlugin
{
    public class Types
    {
        public bool String { get; set; }
        public bool Integer { get; set; }
        public bool Number { get; set; }
        public bool Datetime { get; set; }
        public bool Boolean { get; set; }

        private string _value;
        private bool foundType;

        public Types(string value)
        {
            _value = value;
            foundType = false;
            IsNumber();
            IsInt();
            IsDatetime();
            IsBoolean();
            
            if (!Integer && !Number && !Datetime && !Boolean)
            {
                //if nothing else, it's a string
                String = true;
            }
        }


        private void IsNumber()
        {
            decimal decimalCheck;
            decimal.TryParse(_value, out decimalCheck);
            Number = decimalCheck != 0 ? true : false; 
        }

        private void IsInt()
        {
            int integerCheck;
            int.TryParse(_value, out integerCheck);
            Integer = integerCheck != 0 ? true : false;
        }


        private void IsDatetime()
        {
            DateTime datetimeCheck;
            DateTime.TryParse(_value, out datetimeCheck);
            Datetime = datetimeCheck != DateTime.MinValue ? true : false;
        }

        private void IsBoolean()
        {
            bool booleanCheck;
            bool.TryParse(_value, out booleanCheck);
            Boolean = booleanCheck;
        }

    }


}
