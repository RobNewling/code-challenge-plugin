using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NaveegoGrpcPlugin
{
    public class Types
    {
        public string ColumnName { get; set; }

        public Dictionary<Type, int> TypeVotes { get; private set; }

        private bool typeFound;

        public Types(string columnName, string value)
        {
            ColumnName = columnName;
            SetUpTypes();
            DetectTypes(value);
        }

        private void SetUpTypes()
        {
            TypeVotes = new Dictionary<Type, int>();
            TypeVotes.Add(typeof(int), 0);
            TypeVotes.Add(typeof(decimal), 0);
            TypeVotes.Add(typeof(DateTime), 0);
            TypeVotes.Add(typeof(bool), 0);
            TypeVotes.Add(typeof(string), 0);
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
                TypeVotes[typeof(decimal)] = TypeVotes[typeof(decimal)] + 1;
                typeFound = true;
            }
        }

        private void VoteForInt(string value)
        {
            int integerCheck;
            int.TryParse(value, out integerCheck);
            if (integerCheck != 0)
            {
                TypeVotes[typeof(int)] = TypeVotes[typeof(int)] + 1;
                typeFound = true;
            }
        }


        private void VoteForDatetime(string value)
        {
            DateTime datetimeCheck;
            DateTime.TryParse(value, out datetimeCheck);
            if (datetimeCheck != DateTime.MinValue)
            {
                TypeVotes[typeof(DateTime)] = TypeVotes[typeof(DateTime)] + 1;
                typeFound = true;
            }
        }

        private void VoteForBoolean(string value)
        {
            bool booleanCheck;
            bool.TryParse(value, out booleanCheck);
            if (booleanCheck)
            {
                TypeVotes[typeof(bool)] = TypeVotes[typeof(bool)] + 1;
                typeFound = true;
            }
        }

        private void VoteForString()
        {
            TypeVotes[typeof(string)] = TypeVotes[typeof(string)] + 1;
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
                return "datetime";
            else if (type == typeof(bool))
                return "boolean";
            else
                return string.Empty;
        }

       

    }


}
