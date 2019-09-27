using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NaveegoGrpcPlugin
{
    public class Types
    {
        public string ColumnName { get; set; }
        public int String { get; set; }
        public int Integer { get; set; }
        public int Number { get; set; }
        public int Datetime { get; set; }
        public int Boolean { get; set; }

        private bool typeFound;

        public Types(string columnName, string value)
        {
            ColumnName = columnName;
            DetectTypes(value);
        }

        public void DetectTypes(string value)
        {
            typeFound = false;
            VoteForNumber(value);
            VoteForInt(value);
            VoteForDatetime(value);
            VoteForBoolean(value);
            if (!typeFound)
            {
                VoteForString();
            }
        }


        private void VoteForNumber(string value)
        {
            decimal decimalCheck;
            decimal.TryParse(value, out decimalCheck);
            if (decimalCheck != 0)
            {
                Number = Number + 1;
                typeFound = true;
            }
        }

        private void VoteForInt(string value)
        {
            int integerCheck;
            int.TryParse(value, out integerCheck);
            if (integerCheck != 0)
            {
                Integer = Integer + 1;
                typeFound = true;
            }
        }


        private void VoteForDatetime(string value)
        {
            DateTime datetimeCheck;
            DateTime.TryParse(value, out datetimeCheck);
            if (datetimeCheck != DateTime.MinValue)
            {
                Datetime = Datetime + 1;
                typeFound = true;
            }
        }

        private void VoteForBoolean(string value)
        {
            bool booleanCheck;
            bool.TryParse(value, out booleanCheck);
            if (booleanCheck)
            {
                Boolean = Boolean + 1;
                typeFound = true;
            }
        }

        private void VoteForString()
        {
            String = String + 1;
        }

    }


}
