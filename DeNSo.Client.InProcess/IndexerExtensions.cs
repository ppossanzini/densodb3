using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;



namespace DeNSo
{
  public static class IndexerExtensions
  {
    public static IIndexer<T> For<T, TResult>(this IIndexer<T> item, Expression<Func<T, TResult>> indexexpression)
    {
      Func<object, object> result = (object i) => (object)indexexpression.Compile().Invoke((T)i);
      Session.RegisterIndexer<T>(new Indexer<T>(GetPropertNameFromExpression(indexexpression), result, typeof(TResult)));
      return item;
    }

    private static string GetPropertNameFromExpression<T, TResult>(Expression<Func<T, TResult>> expression)
    {
      Expression expr = expression;
      try
      {
        if (expr.NodeType == ExpressionType.Lambda)
          expr = ((LambdaExpression)expr).Body;

        if (expr.NodeType == ExpressionType.Convert)
          expr = ((UnaryExpression)expr).Operand;

        if (expr.NodeType == ExpressionType.MemberAccess)
          return ((MemberExpression)expr).Member.Name;
      }
      catch
      { } return expr.ToString();
    }
  }
}
