using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Moofile;

/// <summary>
/// A strongly typed MooFile filter. Build one with <see cref="Builders{TDocument}"/>.
/// </summary>
/// <typeparam name="TDocument">
/// A CLR type whose public property names correspond to fields in a MooFile document.
/// </typeparam>
public sealed class FilterDefinition<TDocument>
{
    private readonly Document _document;

    internal FilterDefinition(Document document) => _document = document;

    /// <summary>
    /// Return the MongoDB-style document sent to MooFile. This is useful when
    /// interoperating with APIs that accept an untyped <see cref="Document"/>.
    /// </summary>
    public Document ToDocument() => _document;
}

/// <summary>Entry point for strongly typed filter builders.</summary>
/// <typeparam name="TDocument">The CLR type used to select field names.</typeparam>
/// <remarks>
/// This is an expression-based convenience API over MooFile's supported filter
/// operators; it is not a general LINQ provider. Selectors must be direct,
/// top-level properties, such as <c>person =&gt; person.Age</c>.
/// </remarks>
public static class Builders<TDocument>
{
    /// <summary>Build filters for <typeparamref name="TDocument"/>.</summary>
    public static FilterDefinitionBuilder<TDocument> Filter { get; } = new();
}

/// <summary>Build MongoDB-style filters using strongly typed property selectors.</summary>
public sealed class FilterDefinitionBuilder<TDocument>
{
    /// <summary>Match a field exactly.</summary>
    public FilterDefinition<TDocument> Eq<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        Field(field, value);

    /// <summary>Match a field that does not equal a value.</summary>
    public FilterDefinition<TDocument> Ne<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        FieldOperator(field, "$ne", value);

    public FilterDefinition<TDocument> Gt<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        FieldOperator(field, "$gt", value);

    public FilterDefinition<TDocument> Gte<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        FieldOperator(field, "$gte", value);

    public FilterDefinition<TDocument> Lt<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        FieldOperator(field, "$lt", value);

    public FilterDefinition<TDocument> Lte<TField>(Expression<Func<TDocument, TField>> field, TField value) =>
        FieldOperator(field, "$lte", value);

    public FilterDefinition<TDocument> In<TField>(Expression<Func<TDocument, TField>> field, IEnumerable<TField> values) =>
        FieldOperator(field, "$in", values);

    public FilterDefinition<TDocument> Nin<TField>(Expression<Func<TDocument, TField>> field, IEnumerable<TField> values) =>
        FieldOperator(field, "$nin", values);

    /// <summary>Match documents where a field is present (or absent).</summary>
    public FilterDefinition<TDocument> Exists<TField>(Expression<Func<TDocument, TField>> field, bool exists = true) =>
        FieldOperator(field, "$exists", exists);

    /// <summary>Match an array containing an element that satisfies <paramref name="filter"/>.</summary>
    public FilterDefinition<TDocument> ElemMatch<TCollection, TItem>(
        Expression<Func<TDocument, TCollection>> field,
        FilterDefinition<TItem> filter)
        where TCollection : IEnumerable<TItem> =>
        FieldOperator(field, "$elemMatch", filter?.ToDocument() ?? throw new ArgumentNullException(nameof(filter)));

    /// <summary>Match documents satisfying every supplied filter.</summary>
    public FilterDefinition<TDocument> And(params FilterDefinition<TDocument>[] filters) =>
        Logical("$and", filters);

    /// <summary>Match documents satisfying at least one supplied filter.</summary>
    public FilterDefinition<TDocument> Or(params FilterDefinition<TDocument>[] filters) =>
        Logical("$or", filters);

    /// <summary>Invert a filter.</summary>
    public FilterDefinition<TDocument> Not(FilterDefinition<TDocument> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return new FilterDefinition<TDocument>(Document.Of("$not", filter.ToDocument()));
    }

    private static FilterDefinition<TDocument> Field<TField>(
        Expression<Func<TDocument, TField>> field, TField value) =>
        new(Document.Of(FieldName(field), value));

    private static FilterDefinition<TDocument> FieldOperator<TField>(
        Expression<Func<TDocument, TField>> field, string op, object? value) =>
        new(Document.Of(FieldName(field), Document.Of(op, value)));

    private static FilterDefinition<TDocument> Logical(string op, IEnumerable<FilterDefinition<TDocument>> filters)
    {
        ArgumentNullException.ThrowIfNull(filters);
        var docs = filters.Select(filter =>
            filter?.ToDocument() ?? throw new ArgumentException("filters cannot contain null", nameof(filters)))
            .Cast<object?>().ToList();
        return new FilterDefinition<TDocument>(Document.Of(op, docs));
    }

    private static string FieldName<TField>(Expression<Func<TDocument, TField>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Expression body = selector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
            body = convert.Operand;

        if (body is not MemberExpression { Expression: ParameterExpression } member)
        {
            throw new ArgumentException(
                "Field selectors must be direct top-level properties, for example person => person.Age.",
                nameof(selector));
        }
        return member.Member.Name;
    }
}
