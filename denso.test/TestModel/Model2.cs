using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeNSo.test.TestModel
{
  public class Model2
  {
    public string _Id { get; set; }
    public string MyProperty { get; set; }
    public int MyProperty2 { get; set; }
    public DateTime MyProperty3 { get; set; }

    public TestModel.Model1 MyProperty4 { get; set; }
  }
}
