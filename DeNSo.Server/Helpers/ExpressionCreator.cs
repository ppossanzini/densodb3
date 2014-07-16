using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Web;

namespace DeNSo.Server.Helpers
{
  public class ExpressionCreator : CustomCreationConverter<Expression>
  {
    public override Expression Create(Type objectType)
    {
      return null;
    }
  }
}