using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SharePointTestApp {

    public struct ResultError {

        private readonly string _errorMessage;

        public ResultError(string errorMessage) {
            _errorMessage = errorMessage;
            Exception = null;
        }

        public ResultError(Exception exception) {
            _errorMessage = null;
            Exception = exception;
        }


        public string ErrorMessage {
            get {
                if (Exception != null) {
                    return Exception.Message;
                }
                return _errorMessage ?? string.Empty;
            }
        }

        public Exception Exception { get; }

        public static implicit operator ResultError(string value) => new ResultError(value);
        public static implicit operator ResultError(Exception value) => new ResultError(value);

    }

    public interface IResult {
        bool HasErrors { get; }
        IEnumerable<ResultError> Errors { get; }
        Type ValueType { get; }
        Object GetValue();

        /// <summary>
        /// Returns all errors as a single string, separated by spaces. This will not be an empty string or whitespace.
        /// </summary>
        string ErrorMessageSingleLine { get; }

        /// <summary>
        /// Returns all errors as a single string, separated by newlines. This will not be an empty string or whitespace.
        /// </summary>
        string ErrorMessageMultiLine { get; }

        /// <summary>
        /// Returns all errors as a single string, separated by <br/>. This will not be an empty string or whitespace.
        /// </summary>
        string ErrorMessageHtml { get; }
    }

    public class Result<T> : IResult {
        protected Result() {
            HasErrors = false;
            Value = default;
        }

        public Result(T value) {
            HasErrors = false;
            Value = value;
            if (Value != null) {
                ValueType = Value.GetType();
            }
        }
        protected Result(bool hasErrors) {
            HasErrors = hasErrors;
            Value = default;
        }

        public static implicit operator T(Result<T> result) => result.Value;
        public static implicit operator Result<T>(T value) => new Result<T>(value);

        public static implicit operator Result<T>(Result result) => new Result<T>(default(T)) {
            HasErrors = result.HasErrors,
            Errors = result.Errors
        };

        public override bool Equals(object obj) {
            if (!(obj is Result<T>)) {
                return base.Equals(obj);
            }
            var other = obj as Result<T>;
            if (other != null) {
                if (other.HasErrors && HasErrors == false) {
                    return false;
                }
                if (other.HasErrors == false && HasErrors) {
                    return false;
                }
                if (other.HasErrors && HasErrors) {
                    return false;
                }
                if (Object.Equals(Value, other.Value)) {
                    return true;
                }
            }
            return base.Equals(obj);
        }

        public override int GetHashCode() {
            if (HasErrors) {
                return base.GetHashCode();
            }
            if (Value != null) {
                return Value.GetHashCode();
            }
            return base.GetHashCode();
        }

        public override string ToString() {
            if (HasErrors) {
                return base.ToString();
            }
            if (Value != null) {
                return Value.ToString();
            }
            return base.ToString();
        }

        public T Value { get; }

        public T ValueOrDefault(T defaultValue = default) {
            if (HasErrors) {
                return defaultValue;
            }
            return Value;
        }

        public Type ValueType { get; private set; }

        public bool HasErrors { get; private set; }

        public IEnumerable<ResultError> Errors { get; protected set; } = new List<ResultError>();

        public void ThrowIfError() {
            if (HasErrors) {
                if (Errors.Count() == 1 && Errors.First().Exception != null) {
                    throw Errors.First().Exception;
                }
                throw new InvalidOperationException(ErrorMessageSingleLine);
            }
        }

        public string ErrorMessageSingleLine {
            get {
                return string.Join(" ", Errors.Where(e => string.IsNullOrWhiteSpace(e.ErrorMessage) == false).Select(e => e.ErrorMessage));
            }
        }

        public string ErrorMessageMultiLine {
            get {
                return string.Join("\r\n", Errors.Where(e => string.IsNullOrWhiteSpace(e.ErrorMessage) == false).Select(e => e.ErrorMessage));
            }
        }

        public string ErrorMessageHtml {
            get {
                return string.Join("<br/>", Errors.Where(e => string.IsNullOrWhiteSpace(e.ErrorMessage) == false).Select(e => e.ErrorMessage));
            }
        }

        public void ClearValueType() => ValueType = null;

        /// <summary>
        /// Returns the current value.
        /// </summary>
        /// <returns></returns>
        public object GetValue() {
            return this.Value;
        }

        /// <summary>
        /// Represents a successful operation and accepts a values as the result of the operation
        /// </summary>
        /// <param name="value">Sets the Value property</param>
        /// <returns>A Result<typeparamref name="T"/></returns>
        public static Result<T> Successful(T value) {
            return new Result<T>(value);
        }

        /// <summary>
        /// Represents an error that occurred during the execution of the service.
        /// Error messages may be provided and will be exposed via the Errors property.
        /// </summary>
        /// <param name="errorMessages">A list of string error messages.</param>
        /// <returns>A Result<typeparamref name="T"/></returns>
        public static Result<T> Error(params ResultError[] errorMessages) {
            return new Result<T>(true) { Errors = errorMessages };
        }

    }
    public class Result : Result<Result> {
        public Result() : base() { }

        protected internal Result(bool hasErrors) : base(hasErrors) { }

        /// <summary>
        /// Represents a successful operation without return type
        /// </summary>
        /// <returns>A Result</returns>
        public static Result Successful() {
            return new Result();
        }

        /// <summary>
        /// Represents a successful operation and accepts a values as the result of the operation
        /// </summary>
        /// <param name="value">Sets the Value property</param>
        /// <returns>A Result<typeparamref name="T"/></returns>
        public static Result<T> Successful<T>(T value) {
            return new Result<T>(value);
        }

        /// <summary>
        /// Represents an error that occurred during the execution of the service.
        /// Error messages may be provided and will be exposed via the Errors property.
        /// </summary>
        /// <param name="errorMessages">A list of string error messages.</param>
        /// <returns>A Result</returns>
        public new static Result Error(params ResultError[] errorMessages) {
            return new Result(true) { Errors = errorMessages };
        }

        /// <summary>
        /// Represents an error that occurred during the execution of the service.
        /// Error messages may be provided and will be exposed via the Errors property.
        /// </summary>
        /// <param name="errorMessages">A list of string error messages.</param>
        /// <returns>A Result</returns>
        public static Result Error(IEnumerable<ResultError> errorMessages) {
            return new Result(true) { Errors = errorMessages };
        }

        /// <summary>
        /// Represents an error that occurred during the execution of the service.
        /// Error messages may be provided and will be exposed via the Errors property.
        /// </summary>
        /// <param name="errorMessages">A list of string error messages.</param>
        /// <returns>A Result</returns>
        public static Result Error(IEnumerable<string> errorMessages) {
            return new Result(true) { Errors = errorMessages.Select(e => (ResultError)e) };
        }

    }
}
