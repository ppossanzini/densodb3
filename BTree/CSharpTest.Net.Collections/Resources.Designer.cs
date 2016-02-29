//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.18052
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace CSharpTest.Net
{
    using System;
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This partial class was auto-generated by the StronglyTypedResourceBuilder
    // partial class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "4.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal partial class Resources
    {
        private static global::System.Resources.ResourceManager resourceMan;
        private static global::System.Globalization.CultureInfo resourceCulture;
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources()
        {
        }
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager
        {
            get
            {
                if (object.ReferenceEquals(resourceMan, null))
                {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("CSharpTest.Net.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture
        {
            get
            {
                return resourceCulture;
            }
            set
            {
                resourceCulture = value;
            }
        }
        /// <summary>
        ///   Looks up a localized string similar to Failed to compress/decompress the full input stream..
        /// </summary>
        internal static string IOStreamCompressionFailed
        {
            get
            {
                return ResourceManager.GetString("IOStreamCompressionFailed", resourceCulture);
            }
        }
        /// <summary>
        ///   Looks up a localized string similar to Failed to read from input stream..
        /// </summary>
        internal static string IOStreamFailedToRead
        {
            get
            {
                return ResourceManager.GetString("IOStreamFailedToRead", resourceCulture);
            }
        }
        /// <summary>
        ///   Looks up a localized string similar to The running process must first exit..
        /// </summary>
        internal static string ProcessRunnerAlreadyRunning
        {
            get
            {
                return ResourceManager.GetString("ProcessRunnerAlreadyRunning", resourceCulture);
            }
        }
  

        /// <summary>
        /// The singleton for type {0} threw an excpetion.
        /// </summary>
        public static string FailedToConstructSingleton(System.Type type)
        {
            return String.Format(resourceCulture, CSharpTest.Net.Resources.FormatStrings.FailedToConstructSingleton_System_Type_type_, type);
        }
        /// <summary>
        /// Invalid file extension: &#39;{0}&#39;.
        /// </summary>
        public static string InvalidFileExtension(string ext)
        {
            return String.Format(resourceCulture, CSharpTest.Net.Resources.FormatStrings.InvalidFileExtension_string_ext_, ext);
        }
        /// <summary>
        /// The type {0} is not convertable from a string.
        /// </summary>
        public static string StringConverterTryParse(System.Type type)
        {
            return String.Format(resourceCulture, CSharpTest.Net.Resources.FormatStrings.StringConverterTryParse_System_Type_type_, type);
        }
        
        /// <summary>
        /// Returns the raw format strings.
        /// </summary>
        [global::System.Diagnostics.DebuggerStepThroughAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
        public static partial class FormatStrings
        {
            /// <summary>
            /// The singleton for type {0} threw an excpetion.
            /// </summary>
            public static string FailedToConstructSingleton_System_Type_type_ { get { return ResourceManager.GetString("FailedToConstructSingleton(System.Type type)", resourceCulture); } }
            /// <summary>
            /// Invalid file extension: &#39;{0}&#39;.
            /// </summary>
            public static string InvalidFileExtension_string_ext_ { get { return ResourceManager.GetString("InvalidFileExtension(string ext)", resourceCulture); } }
            /// <summary>
            /// The type {0} is not convertable from a string.
            /// </summary>
            public static string StringConverterTryParse_System_Type_type_ { get { return ResourceManager.GetString("StringConverterTryParse(System.Type type)", resourceCulture); } }
        }
        
        /// <summary>
        /// Returns the raw exception strings.
        /// </summary>
        [global::System.Diagnostics.DebuggerStepThroughAttribute()]
        [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
        [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
        [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
        public static partial class ExceptionStrings
        {
            /// <summary>
            /// Formats a message for an exception
            /// </summary>
            internal static string SafeFormat(string message, params object[] args)
            {
                try
                {
                    return string.Format(resourceCulture, message, args);
                }
                catch
                {
                    return message ?? string.Empty;
                }
            }
            /// <summary>
            /// </summary>
            internal static string HelpLinkFormat(int hResult, string typeName)
            {
                return SafeFormat("", hResult, typeName);
            }
            /// <summary>
            /// A runtime assertion failed while performing the operation.
            /// </summary>
            public static string AssertionFailedException { get { return ResourceManager.GetString("AssertionFailedException", resourceCulture); } }
            /// <summary>
            /// A runtime assertion failed: {0}
            /// </summary>
            public static string AssertionFailedException_string_message_ { get { return ResourceManager.GetString("AssertionFailedException(string message)", resourceCulture); } }
            /// <summary>
            /// A lock timeout has expired due to a possible deadlock.
            /// </summary>
            public static string DeadlockException { get { return ResourceManager.GetString("DeadlockException", resourceCulture); } }
            /// <summary>
            /// Debug Assertion Failed: {0}
            /// </summary>
            public static string DebugAssertionFailedException_string_message_ { get { return ResourceManager.GetString("DebugAssertionFailedException(string message)", resourceCulture); } }
            /// <summary>
            /// The specified key already exists in the collection.
            /// </summary>
            public static string DuplicateKeyException { get { return ResourceManager.GetString("DuplicateKeyException", resourceCulture); } }
            /// <summary>
            /// The configuration value &#39;{0}&#39; is invalid.
            /// </summary>
            public static string InvalidConfigurationValueException_string_property_ { get { return ResourceManager.GetString("InvalidConfigurationValueException(string property)", resourceCulture); } }
            /// <summary>
            /// The configuration value &#39;{0}&#39; is invalid.
            /// {1}
            /// </summary>
            public static string InvalidConfigurationValueException_string_property__string_message_ { get { return ResourceManager.GetString("InvalidConfigurationValueException(string property, string message)", resourceCulture); } }
            /// <summary>
            /// A storage handle was invalid or has been corrupted.
            /// </summary>
            public static string InvalidNodeHandleException { get { return ResourceManager.GetString("InvalidNodeHandleException", resourceCulture); } }
            /// <summary>
            /// The LurchTable internal datastructure appears to be corrupted.
            /// </summary>
            public static string LurchTableCorruptionException { get { return ResourceManager.GetString("LurchTableCorruptionException", resourceCulture); } }
        }
    
     }

    /// <summary>
    /// Exception class: AssertionFailedException
    /// A runtime assertion failed while performing the operation.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class AssertionFailedException : BaseAssertionException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected AssertionFailedException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new AssertionFailedException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected AssertionFailedException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// A runtime assertion failed while performing the operation.
        /// </summary>
        public AssertionFailedException()
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.AssertionFailedException)
        {
        }
        /// <summary>
        /// A runtime assertion failed while performing the operation.
        /// </summary>
        public AssertionFailedException(System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.AssertionFailedException)
        {
        }
        /// <summary>
        /// if(condition == false) throws A runtime assertion failed while performing the operation.
        /// </summary>
        public static void Assert(bool condition)
        {
            if (!condition) throw new AssertionFailedException();
        }
        /// <summary>
        /// A runtime assertion failed: {0}
        /// </summary>
        public AssertionFailedException(string message)
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.AssertionFailedException_string_message_, message))
        {
        }
        /// <summary>
        /// A runtime assertion failed: {0}
        /// </summary>
        public AssertionFailedException(string message, System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.AssertionFailedException_string_message_, message))
        {
        }
        /// <summary>
        /// if(condition == false) throws A runtime assertion failed: {0}
        /// </summary>
        public static void Assert(bool condition, string message)
        {
            if (!condition) throw new AssertionFailedException(message);
        }
    }
    /// <summary>
    /// Exception class: DeadlockException
    /// A lock timeout has expired due to a possible deadlock.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class DeadlockException : BaseAssertionException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected DeadlockException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new DeadlockException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected DeadlockException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// A lock timeout has expired due to a possible deadlock.
        /// </summary>
        public DeadlockException()
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.DeadlockException)
        {
        }
        /// <summary>
        /// A lock timeout has expired due to a possible deadlock.
        /// </summary>
        public DeadlockException(System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.DeadlockException)
        {
        }
        /// <summary>
        /// if(condition == false) throws A lock timeout has expired due to a possible deadlock.
        /// </summary>
        public static void Assert(bool condition)
        {
            if (!condition) throw new DeadlockException();
        }
    }
    /// <summary>
    /// Exception class: DebugAssertionFailedException
    /// Debug Assertion Failed: {0}
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class DebugAssertionFailedException : System.ApplicationException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected DebugAssertionFailedException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new DebugAssertionFailedException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected DebugAssertionFailedException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// Debug Assertion Failed: {0}
        /// </summary>
        public DebugAssertionFailedException(string message)
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.DebugAssertionFailedException_string_message_, message))
        {
        }
        /// <summary>
        /// Debug Assertion Failed: {0}
        /// </summary>
        public DebugAssertionFailedException(string message, System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.DebugAssertionFailedException_string_message_, message))
        {
        }
        /// <summary>
        /// if(condition == false) throws Debug Assertion Failed: {0}
        /// </summary>
        public static void Assert(bool condition, string message)
        {
            if (!condition) throw new DebugAssertionFailedException(message);
        }
    }
    /// <summary>
    /// Exception class: DuplicateKeyException
    /// The specified key already exists in the collection.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class DuplicateKeyException : System.ApplicationException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected DuplicateKeyException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new DuplicateKeyException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected DuplicateKeyException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// The specified key already exists in the collection.
        /// </summary>
        public DuplicateKeyException()
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.DuplicateKeyException)
        {
        }
        /// <summary>
        /// The specified key already exists in the collection.
        /// </summary>
        public DuplicateKeyException(System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.DuplicateKeyException)
        {
        }
        /// <summary>
        /// if(condition == false) throws The specified key already exists in the collection.
        /// </summary>
        public static void Assert(bool condition)
        {
            if (!condition) throw new DuplicateKeyException();
        }
    }
    /// <summary>
    /// Exception class: InvalidConfigurationValueException
    /// The configuration value &#39;{0}&#39; is invalid.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class InvalidConfigurationValueException : ArgumentException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected InvalidConfigurationValueException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new InvalidConfigurationValueException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected InvalidConfigurationValueException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// The configuration value &#39;{0}&#39; is invalid.
        /// </summary>
        public InvalidConfigurationValueException(string property)
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.InvalidConfigurationValueException_string_property_, property))
        {
        }
        /// <summary>
        /// The configuration value &#39;{0}&#39; is invalid.
        /// </summary>
        public InvalidConfigurationValueException(string property, System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.InvalidConfigurationValueException_string_property_, property))
        {
        }
        /// <summary>
        /// if(condition == false) throws The configuration value &#39;{0}&#39; is invalid.
        /// </summary>
        public static void Assert(bool condition, string property)
        {
            if (!condition) throw new InvalidConfigurationValueException(property);
        }
        /// <summary>
        /// The configuration value &#39;{0}&#39; is invalid.
        /// {1}
        /// </summary>
        public InvalidConfigurationValueException(string property, string message)
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.InvalidConfigurationValueException_string_property__string_message_, property, message))
        {
        }
        /// <summary>
        /// The configuration value &#39;{0}&#39; is invalid.
        /// {1}
        /// </summary>
        public InvalidConfigurationValueException(string property, string message, System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.SafeFormat(CSharpTest.Net.Resources.ExceptionStrings.InvalidConfigurationValueException_string_property__string_message_, property, message))
        {
        }
        /// <summary>
        /// if(condition == false) throws The configuration value &#39;{0}&#39; is invalid.
        /// {1}
        /// </summary>
        public static void Assert(bool condition, string property, string message)
        {
            if (!condition) throw new InvalidConfigurationValueException(property, message);
        }
    }
    /// <summary>
    /// Exception class: InvalidNodeHandleException
    /// A storage handle was invalid or has been corrupted.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class InvalidNodeHandleException : BaseAssertionException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected InvalidNodeHandleException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new InvalidNodeHandleException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected InvalidNodeHandleException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// A storage handle was invalid or has been corrupted.
        /// </summary>
        public InvalidNodeHandleException()
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.InvalidNodeHandleException)
        {
        }
        /// <summary>
        /// A storage handle was invalid or has been corrupted.
        /// </summary>
        public InvalidNodeHandleException(System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.InvalidNodeHandleException)
        {
        }
        /// <summary>
        /// if(condition == false) throws A storage handle was invalid or has been corrupted.
        /// </summary>
        public static void Assert(bool condition)
        {
            if (!condition) throw new InvalidNodeHandleException();
        }
    }
    /// <summary>
    /// Exception class: LurchTableCorruptionException
    /// The LurchTable internal datastructure appears to be corrupted.
    /// </summary>
    [System.SerializableAttribute()]
    [global::System.Diagnostics.DebuggerStepThroughAttribute()]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("CSharpTest.Net.Generators", "2.13.222.435")]
    public partial class LurchTableCorruptionException : System.ApplicationException
    {
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected LurchTableCorruptionException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context)
        {
        }
        /// <summary>
        /// Used to create this exception from an hresult and message bypassing the message formatting
        /// </summary>
        internal static System.Exception Create(int hResult, string message)
        {
            return new LurchTableCorruptionException((System.Exception)null, hResult, message);
        }
        /// <summary>
        /// Constructs the exception from an hresult and message bypassing the message formatting
        /// </summary>
        protected LurchTableCorruptionException(System.Exception innerException, int hResult, string message) : base(message, innerException)
        {
            base.HResult = hResult;
            base.HelpLink = CSharpTest.Net.Resources.ExceptionStrings.HelpLinkFormat(HResult, GetType().FullName);
        }
        /// <summary>
        /// The LurchTable internal datastructure appears to be corrupted.
        /// </summary>
        public LurchTableCorruptionException()
        	: this((System.Exception)null, -1, CSharpTest.Net.Resources.ExceptionStrings.LurchTableCorruptionException)
        {
        }
        /// <summary>
        /// The LurchTable internal datastructure appears to be corrupted.
        /// </summary>
        public LurchTableCorruptionException(System.Exception innerException)
        	: this(innerException, -1, CSharpTest.Net.Resources.ExceptionStrings.LurchTableCorruptionException)
        {
        }
        /// <summary>
        /// if(condition == false) throws The LurchTable internal datastructure appears to be corrupted.
        /// </summary>
        public static void Assert(bool condition)
        {
            if (!condition) throw new LurchTableCorruptionException();
        }
    }


}

