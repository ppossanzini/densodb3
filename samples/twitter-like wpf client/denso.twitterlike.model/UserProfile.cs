using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace denso.twitterlike.model
{
  public class UserProfile
  {

    public string _Id { get; set; }

    public string UserName { get { return _Id; } set { _Id = value; } }
    public string UserFullName { get; set; }
    public string Description { get; set; }

    public List<string> Following { get; set; }
    public List<string> FollowedBy { get; set; }

  }
}
