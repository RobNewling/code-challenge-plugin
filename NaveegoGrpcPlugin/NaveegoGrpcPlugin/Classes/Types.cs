using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NaveegoGrpcPlugin
{
    public class Types
    {
        public string ColumnName { get; set; }

        public Dictionary<Type, int> TypeVotes { 
            get
            {
                return typeDict;
            }
        }

        private bool typeFound;
        private Dictionary<Type, int> typeDict;

        public Types(string columnName, string value)
        {
            
            ColumnName = columnName;
            SetUpTypes();
            DetectTypes(value);
        }

        private void SetUpTypes()
        {
            typeDict = new Dictionary<Type, int>();
            typeDict.Add(typeof(int), 0);
            typeDict.Add(typeof(decimal), 0);
            typeDict.Add(typeof(DateTime), 0);
            typeDict.Add(typeof(bool), 0);
            typeDict.Add(typeof(string), 0);
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
                typeDict[typeof(decimal)] = typeDict[typeof(decimal)] + 1;
                typeFound = true;
            }
        }

        private void VoteForInt(string value)
        {
            int integerCheck;
            int.TryParse(value, out integerCheck);
            if (integerCheck != 0)
            {
                typeDict[typeof(int)] = typeDict[typeof(int)] + 1;
                typeFound = true;
            }
        }


        private void VoteForDatetime(string value)
        {
            DateTime datetimeCheck;
            DateTime.TryParse(value, out datetimeCheck);
            if (datetimeCheck != DateTime.MinValue)
            {
                typeDict[typeof(DateTime)] = typeDict[typeof(DateTime)] + 1;
                typeFound = true;
            }
        }

        private void VoteForBoolean(string value)
        {
            bool booleanCheck;
            bool.TryParse(value, out booleanCheck);
            if (booleanCheck)
            {
                typeDict[typeof(bool)] = typeDict[typeof(bool)] + 1;
                typeFound = true;
            }
        }

        private void VoteForString()
        {
            typeDict[typeof(string)] = typeDict[typeof(string)] + 1;
        }

        public string TypeNameConvert(Type type)
        {
            if (type == typeof(string))
                return "string";
            else if (type == typeof(int))
                return "integer";
            else if (type == typeof(decimal))
                return "number";
            else if (type == typeof(DateTime))
                return "datetame";
            else if (type == typeof(bool))
                return "boolean";
            else
                return string.Empty;
        }

    }


}
